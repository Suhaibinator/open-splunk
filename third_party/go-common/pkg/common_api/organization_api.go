package common_api

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RegisterOrganizationRoutes RegisterOrganizationRoutes registers API routes and wires this capability into the backend request flow during handler setup.
func (h *CommonAPIHandler[U]) RegisterOrganizationRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group = h.featureGroup(group, config.APIView.EnableOrganizations)

	// Get transaction middleware for write operations
	transactionMiddleware := h.GetTransactionMiddleware()

	// Check Organization Slug Availability (unauthenticated)
	checkSlugRoute := router.RouteConfig[*proto_models.CheckOrganizationSlugAvailabilityRequest, *proto_models.CheckOrganizationSlugAvailabilityResponse]{
		Path:       "/organizations/check-slug",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.NoAuth),
		Codec:      codec.NewProtoCodec[*proto_models.CheckOrganizationSlugAvailabilityRequest, *proto_models.CheckOrganizationSlugAvailabilityResponse](),
		Handler:    h.HandleCheckOrganizationSlugAvailability,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeCheckOrganizationSlugAvailabilityRequest,
	}
	group.Route(checkSlugRoute)

	// Create Organization
	createOrgRoute := router.RouteConfig[*proto_models.CreateOrganizationRequest, *proto_models.CreateOrganizationResponse]{
		Path:        "/organizations",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.CreateOrganizationRequest, *proto_models.CreateOrganizationResponse](),
		Handler:     h.CreateOrganizationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeCreateOrganizationRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(createOrgRoute)

	// Get Organization by ID
	getOrgRoute := router.RouteConfig[*proto_models.GetOrganizationRequest, *proto_models.GetOrganizationResponse]{
		Path:       "/organizations/get",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetOrganizationRequest, *proto_models.GetOrganizationResponse](),
		Handler:    h.GetOrganizationGeneric,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetOrganizationRequest,
	}
	group.Route(getOrgRoute)

	// Get Organization by Slug
	getOrgBySlugRoute := router.RouteConfig[*proto_models.GetOrganizationBySlugRequest, *proto_models.GetOrganizationBySlugResponse]{
		Path:       "/organizations/get-by-slug",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetOrganizationBySlugRequest, *proto_models.GetOrganizationBySlugResponse](),
		Handler:    h.GetOrganizationBySlugGeneric,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetOrganizationBySlugRequest,
	}
	group.Route(getOrgBySlugRoute)

	// Get Current User's Role in Organization (by id or slug)
	getMyRoleRoute := router.RouteConfig[*proto_models.GetMyOrganizationRoleRequest, *proto_models.GetMyOrganizationRoleResponse]{
		Path:       "/organizations/role/get-my",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetMyOrganizationRoleRequest, *proto_models.GetMyOrganizationRoleResponse](),
		Handler:    h.GetMyOrganizationRoleGeneric,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetMyOrganizationRoleRequest,
	}
	group.Route(getMyRoleRoute)

	// Update Organization
	updateOrgRoute := router.RouteConfig[*proto_models.UpdateOrganizationRequest, *proto_models.UpdateOrganizationResponse]{
		Path:        "/organizations/update",
		Methods:     []router.HttpMethod{http.MethodPut, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.UpdateOrganizationRequest, *proto_models.UpdateOrganizationResponse](),
		Handler:     h.UpdateOrganizationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUpdateOrganizationRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(updateOrgRoute)

	// Delete Organization
	deleteOrgRoute := router.RouteConfig[*proto_models.DeleteOrganizationRequest, *proto_models.DeleteOrganizationResponse]{
		Path:        "/organizations/delete",
		Methods:     []router.HttpMethod{http.MethodDelete, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.DeleteOrganizationRequest, *proto_models.DeleteOrganizationResponse](),
		Handler:     h.DeleteOrganizationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeDeleteOrganizationRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(deleteOrgRoute)

	// Add User to Organization
	addUserRoute := router.RouteConfig[*proto_models.AddUserToOrganizationRequest, *proto_models.AddUserToOrganizationResponse]{
		Path:        "/organizations/members/add",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AddUserToOrganizationRequest, *proto_models.AddUserToOrganizationResponse](),
		Handler:     h.AddUserToOrganizationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAddUserToOrganizationRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(addUserRoute)

	// Add User to Organization by Identifier (direct add or auto-add)
	addUserByIdentifierRoute := router.RouteConfig[*proto_models.AddUserToOrganizationByIdentifierRequest, *proto_models.AddUserToOrganizationByIdentifierResponse]{
		Path:        "/organizations/members/add-by-identifier",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AddUserToOrganizationByIdentifierRequest, *proto_models.AddUserToOrganizationByIdentifierResponse](),
		Handler:     h.AddUserToOrganizationByIdentifierGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAddUserToOrganizationByIdentifierRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(addUserByIdentifierRoute)

	// Remove User from Organization
	removeUserRoute := router.RouteConfig[*proto_models.RemoveUserFromOrganizationRequest, *proto_models.RemoveUserFromOrganizationResponse]{
		Path:        "/organizations/members/remove",
		Methods:     []router.HttpMethod{http.MethodDelete, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.RemoveUserFromOrganizationRequest, *proto_models.RemoveUserFromOrganizationResponse](),
		Handler:     h.RemoveUserFromOrganizationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeRemoveUserFromOrganizationRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(removeUserRoute)

	// Update User Role in Organization
	updateRoleRoute := router.RouteConfig[*proto_models.UpdateUserRoleInOrganizationRequest, *proto_models.UpdateUserRoleInOrganizationResponse]{
		Path:        "/organizations/members/update-role",
		Methods:     []router.HttpMethod{http.MethodPut, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.UpdateUserRoleInOrganizationRequest, *proto_models.UpdateUserRoleInOrganizationResponse](),
		Handler:     h.UpdateUserRoleInOrganizationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUpdateUserRoleInOrganizationRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(updateRoleRoute)

	// List Organization Users
	listUsersRoute := router.RouteConfig[*proto_models.ListOrganizationUsersRequest, *proto_models.ListOrganizationUsersResponse]{
		Path:       "/organizations/members/list",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListOrganizationUsersRequest, *proto_models.ListOrganizationUsersResponse](),
		Handler:    h.ListOrganizationUsersGeneric,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListOrganizationUsersRequest,
	}
	group.Route(listUsersRoute)

	// Create Organization Email Domain
	createEmailDomainRoute := router.RouteConfig[*proto_models.CreateOrganizationEmailDomainRequest, *proto_models.CreateOrganizationEmailDomainResponse]{
		Path:        "/organizations/email-domains/add",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.CreateOrganizationEmailDomainRequest, *proto_models.CreateOrganizationEmailDomainResponse](),
		Handler:     h.CreateOrganizationEmailDomainGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeCreateOrganizationEmailDomainRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(createEmailDomainRoute)

	// Delete Organization Email Domain
	deleteEmailDomainRoute := router.RouteConfig[*proto_models.DeleteOrganizationEmailDomainRequest, *proto_models.DeleteOrganizationEmailDomainResponse]{
		Path:        "/organizations/email-domains/remove",
		Methods:     []router.HttpMethod{http.MethodDelete, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.DeleteOrganizationEmailDomainRequest, *proto_models.DeleteOrganizationEmailDomainResponse](),
		Handler:     h.DeleteOrganizationEmailDomainGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeDeleteOrganizationEmailDomainRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(deleteEmailDomainRoute)

	// List Organization Email Domains
	listEmailDomainsRoute := router.RouteConfig[*proto_models.ListOrganizationEmailDomainsRequest, *proto_models.ListOrganizationEmailDomainsResponse]{
		Path:       "/organizations/email-domains/list",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListOrganizationEmailDomainsRequest, *proto_models.ListOrganizationEmailDomainsResponse](),
		Handler:    h.ListOrganizationEmailDomainsGeneric,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListOrganizationEmailDomainsRequest,
	}
	group.Route(listEmailDomainsRoute)

	// List User's Organizations
	listOrganizationsRoute := router.RouteConfig[*proto_models.ListOrganizationsRequest, *proto_models.ListOrganizationsResponse]{
		Path:       "/organizations/list",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListOrganizationsRequest, *proto_models.ListOrganizationsResponse](),
		Handler:    h.ListOrganizationsGeneric,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListOrganizationsRequest,
	}
	group.Route(listOrganizationsRoute)

	// List Organizations By Parent
	listByParentRoute := router.RouteConfig[*proto_models.ListOrganizationsByParentRequest, *proto_models.ListOrganizationsByParentResponse]{
		Path:       "/organizations/list-by-parent",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListOrganizationsByParentRequest, *proto_models.ListOrganizationsByParentResponse](),
		Handler:    h.ListOrganizationsByParentGeneric,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListOrganizationsByParentRequest,
	}
	group.Route(listByParentRoute)

	// Get Organization Hierarchy
	getHierarchyRoute := router.RouteConfig[*proto_models.GetOrganizationHierarchyRequest, *proto_models.GetOrganizationHierarchyResponse]{
		Path:       "/organizations/hierarchy/get",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetOrganizationHierarchyRequest, *proto_models.GetOrganizationHierarchyResponse](),
		Handler:    h.GetOrganizationHierarchyGeneric,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetOrganizationHierarchyRequest,
	}
	group.Route(getHierarchyRoute)

	// Get Individual Organization Member (requires organization member-list permission)
	getOrgMemberRoute := router.RouteConfig[*proto_models.GetOrganizationMemberRequest, *proto_models.GetOrganizationMemberResponse]{
		Path:       "/organizations/members/get",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetOrganizationMemberRequest, *proto_models.GetOrganizationMemberResponse](),
		Handler:    h.GetOrganizationMemberGeneric,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetOrganizationMemberRequest,
	}
	group.Route(getOrgMemberRoute)

	// Invite User to Organization
	inviteUserRoute := router.RouteConfig[*proto_models.InviteUserToOrganizationRequest, *proto_models.InviteUserToOrganizationResponse]{
		Path:        "/organizations/members/invite",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.InviteUserToOrganizationRequest, *proto_models.InviteUserToOrganizationResponse](),
		Handler:     h.InviteUserToOrganizationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeInviteUserToOrganizationRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(inviteUserRoute)

	// List Pending Invitations for Current User
	listInvitationsRoute := router.RouteConfig[*proto_models.ListPendingInvitationsRequest, *proto_models.ListPendingInvitationsResponse]{
		Path:       "/user/invitations/list",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListPendingInvitationsRequest, *proto_models.ListPendingInvitationsResponse](),
		Handler:    h.ListPendingInvitationsGeneric,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListPendingInvitationsRequest,
	}
	group.Route(listInvitationsRoute)

	// Accept Organization Invitation
	acceptInvitationRoute := router.RouteConfig[*proto_models.AcceptInvitationRequest, *proto_models.AcceptInvitationResponse]{
		Path:        "/user/invitations/accept",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AcceptInvitationRequest, *proto_models.AcceptInvitationResponse](),
		Handler:     h.AcceptInvitationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAcceptInvitationRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(acceptInvitationRoute)

	// Decline Organization Invitation
	declineInvitationRoute := router.RouteConfig[*proto_models.DeclineInvitationRequest, *proto_models.DeclineInvitationResponse]{
		Path:        "/user/invitations/decline",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.DeclineInvitationRequest, *proto_models.DeclineInvitationResponse](),
		Handler:     h.DeclineInvitationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeDeclineInvitationRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(declineInvitationRoute)

	// Cancel Organization Invitation (admin action)
	cancelInvitationRoute := router.RouteConfig[*proto_models.CancelInvitationRequest, *proto_models.CancelInvitationResponse]{
		Path:        "/organizations/invitations/cancel",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.CancelInvitationRequest, *proto_models.CancelInvitationResponse](),
		Handler:     h.CancelInvitationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeCancelInvitationRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(cancelInvitationRoute)

	// List Organization Invitations (admin action)
	listOrgInvitationsRoute := router.RouteConfig[*proto_models.ListOrganizationInvitationsRequest, *proto_models.ListOrganizationInvitationsResponse]{
		Path:        "/organizations/invitations/list",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.ListOrganizationInvitationsRequest, *proto_models.ListOrganizationInvitationsResponse](),
		Handler:     h.ListOrganizationInvitationsGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeListOrganizationInvitationsRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(listOrgInvitationsRoute)

	h.logger.Named(commonAPILoggerName).Debug("Organization routes registered to route group")
}

// CreateOrganizationGeneric CreateOrganizationGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) CreateOrganizationGeneric(r *http.Request, req *proto_models.CreateOrganizationRequest) (*proto_models.CreateOrganizationResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Convert proto request to service model
	org := &common_service_models.Organization{
		Name:        req.Name,
		Slug:        req.Slug,
		Description: req.GetDescription(),
		AvatarUrl:   req.GetAvatarUrl(),
	}
	if req.ParentOrganizationId != nil {
		converted := common_service_models.OrganizationIdType(*req.ParentOrganizationId)
		org.ParentOrganizationID = &converted
	}

	// Call service layer with user creation method
	orgResponse, err := h.serviceHandler.GetOrganizationHandler().CreateOrganizationByUser(r.Context(), org)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "create_organization_by_user"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyName, req.Name),
			zap.String(appLogger.KeySlug, req.Slug))
	}

	// Convert service response to proto response
	protoOrg := h.serviceHandler.GetConverter().ServiceOrganizationToProto(orgResponse.Organization)

	return &proto_models.CreateOrganizationResponse{
		Organization: protoOrg,
	}, nil
}

// GetOrganizationGeneric GetOrganizationGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) GetOrganizationGeneric(r *http.Request, req *proto_models.GetOrganizationRequest) (*proto_models.GetOrganizationResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated user ID (may be needed for authorization checks)
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Call service layer
	orgResponse, err := h.serviceHandler.GetOrganizationHandler().GetOrganization(r.Context(), common_service_models.OrganizationIdType(req.OrganizationId))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_organization"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}

	// Convert service response to proto response
	protoOrg := h.serviceHandler.GetConverter().ServiceOrganizationToProto(orgResponse.Organization)

	return &proto_models.GetOrganizationResponse{
		Organization: protoOrg,
	}, nil
}

// GetOrganizationBySlugGeneric GetOrganizationBySlugGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) GetOrganizationBySlugGeneric(r *http.Request, req *proto_models.GetOrganizationBySlugRequest) (*proto_models.GetOrganizationBySlugResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated user ID (may be needed for authorization checks)
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Call service layer
	orgResponse, err := h.serviceHandler.GetOrganizationHandler().GetOrganizationBySlug(r.Context(), req.Slug)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_organization_by_slug"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeySlug, req.Slug))
	}

	// Convert service response to proto response
	protoOrg := h.serviceHandler.GetConverter().ServiceOrganizationToProto(orgResponse.Organization)

	return &proto_models.GetOrganizationBySlugResponse{
		Organization: protoOrg,
	}, nil
}

// GetMyOrganizationRoleGeneric GetMyOrganizationRoleGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) GetMyOrganizationRoleGeneric(r *http.Request, req *proto_models.GetMyOrganizationRoleRequest) (*proto_models.GetMyOrganizationRoleResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	var orgID common_service_models.OrganizationIdType
	var resolvedOrgDepth *uint32
	switch identifier := req.GetIdentifier().(type) {
	case *proto_models.GetMyOrganizationRoleRequest_OrganizationId:
		orgID = common_service_models.OrganizationIdType(identifier.OrganizationId)
	case *proto_models.GetMyOrganizationRoleRequest_Slug:
		orgResponse, err := h.serviceHandler.GetOrganizationHandler().GetOrganizationBySlug(r.Context(), identifier.Slug)
		if err != nil {
			return nil, MapServiceErrorWithFields(err,
				zap.String(logkeys.Operation, "resolve_organization_for_role_lookup"),
				zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.String(appLogger.KeySlug, identifier.Slug))
		}
		if orgResponse == nil || orgResponse.Organization == nil {
			return nil, MapServiceErrorWithFields(
				appLogger.InvariantError("organization_lookup_returns_identity", nil,
					zap.String(logkeys.Stage, "resolve_role_lookup_slug"),
					zap.String(logkeys.Expected, "non-nil organization response"),
					zap.String(logkeys.Actual, "nil organization response")),
				zap.String(logkeys.Operation, "resolve_organization_for_role_lookup"),
				zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.String(appLogger.KeySlug, identifier.Slug))
		}
		orgID = orgResponse.ID
		depth := uint32(len(orgResponse.Lineage))
		resolvedOrgDepth = &depth
	default:
		return nil, ErrInvalidRequest
	}

	roleInfo, isMember, err := h.serviceHandler.GetOrganizationHandler().GetUserRoleInfo(r.Context(), userID, orgID)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_user_organization_role"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, int64(orgID)))
	}

	var inheritedFrom *uint64
	var effectiveRoleDepth *uint32
	if isMember {
		if roleInfo.InheritedFromOrganizationID != nil {
			converted := uint64(*roleInfo.InheritedFromOrganizationID)
			inheritedFrom = &converted
		}
		if roleInfo.EffectiveRoleDepth != nil {
			convertedDepth := *roleInfo.EffectiveRoleDepth
			effectiveRoleDepth = &convertedDepth
		} else if roleInfo.EffectiveRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
			if resolvedOrgDepth == nil {
				orgResponse, orgErr := h.serviceHandler.GetOrganizationHandler().GetOrganization(r.Context(), orgID)
				if orgErr != nil {
					appLogger.For(r.Context(), h.logger, commonAPILoggerName).Warn("Failed to resolve organization depth for role response",
						zap.String(logkeys.Operation, "resolve_organization_depth_for_role_response"),
						zap.Uint64(appLogger.KeyUserID, uint64(userID)),
						zap.Int64(appLogger.KeyOrganizationID, int64(orgID)),
						zap.String(logkeys.Fallback, "omit_effective_role_depth"),
						zap.Error(orgErr))
				} else if orgResponse != nil && orgResponse.Organization != nil {
					depth := uint32(len(orgResponse.Lineage))
					resolvedOrgDepth = &depth
				}
			}
			if resolvedOrgDepth != nil {
				convertedDepth := *resolvedOrgDepth
				effectiveRoleDepth = &convertedDepth
			}
		}
	}

	return &proto_models.GetMyOrganizationRoleResponse{
		ExplicitRole:                roleInfo.ExplicitRole,
		EffectiveRole:               roleInfo.EffectiveRole,
		InheritedFromOrganizationId: inheritedFrom,
		EffectiveRoleDepth:          effectiveRoleDepth,
	}, nil
}

// UpdateOrganizationGeneric UpdateOrganizationGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) UpdateOrganizationGeneric(r *http.Request, req *proto_models.UpdateOrganizationRequest) (*proto_models.UpdateOrganizationResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Convert proto request to service model
	org := &common_service_models.Organization{
		ID:          common_service_models.OrganizationIdType(req.OrganizationId),
		Name:        req.Name,
		Slug:        req.Slug,
		Description: req.GetDescription(),
		AvatarUrl:   req.GetAvatarUrl(),
	}

	// Call service layer
	orgResponse, err := h.serviceHandler.GetOrganizationHandler().UpdateOrganization(r.Context(), org)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "update_organization"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}

	// Convert service response to proto response
	protoOrg := h.serviceHandler.GetConverter().ServiceOrganizationToProto(orgResponse.Organization)

	return &proto_models.UpdateOrganizationResponse{
		Organization: protoOrg,
	}, nil
}

// DeleteOrganizationGeneric DeleteOrganizationGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) DeleteOrganizationGeneric(r *http.Request, req *proto_models.DeleteOrganizationRequest) (*proto_models.DeleteOrganizationResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Call service layer
	err := h.serviceHandler.GetOrganizationHandler().DeleteOrganization(r.Context(), common_service_models.OrganizationIdType(req.OrganizationId))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "delete_organization"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}

	return &proto_models.DeleteOrganizationResponse{}, nil
}

// AddUserToOrganizationGeneric AddUserToOrganizationGeneric supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) AddUserToOrganizationGeneric(r *http.Request, req *proto_models.AddUserToOrganizationRequest) (*proto_models.AddUserToOrganizationResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Call service layer with direct parameters
	memberResponse, err := h.serviceHandler.GetOrganizationHandler().AddMember(
		r.Context(),
		common_service_models.UserIdType(req.UserId),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.Role,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "add_user_to_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId),
			zap.Stringer(appLogger.KeyNewRole, req.Role))
	}

	// Convert service response to proto response
	protoOrgUser := h.convertUserOrganizationToProto(memberResponse.UserOrganization)

	return &proto_models.AddUserToOrganizationResponse{
		OrganizationUser: protoOrgUser,
	}, nil
}

// AddUserToOrganizationByIdentifierGeneric AddUserToOrganizationByIdentifierGeneric supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) AddUserToOrganizationByIdentifierGeneric(r *http.Request, req *proto_models.AddUserToOrganizationByIdentifierRequest) (*proto_models.AddUserToOrganizationByIdentifierResponse, error) {
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	result, err := h.serviceHandler.GetOrganizationHandler().AddUserToOrganizationByIdentifier(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.IdentifierType,
		req.Identifier,
		req.Role,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "add_user_to_organization_by_identifier"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.String(appLogger.KeyIdentifier, req.Identifier),
			zap.Stringer(appLogger.KeyIdentifierType, req.IdentifierType),
			zap.Stringer(appLogger.KeyNewRole, req.Role))
	}

	response := &proto_models.AddUserToOrganizationByIdentifierResponse{
		UserAdded:      result.UserAdded,
		AutoAddPending: result.AutoAddPending,
	}

	if result.OrganizationUser != nil && result.OrganizationUser.UserOrganization != nil {
		response.OrganizationUser = h.convertUserOrganizationToProto(result.OrganizationUser.UserOrganization)
	}
	if result.AutoAddEntry != nil {
		response.AutoAddEntry = h.serviceHandler.GetConverter().ServiceOrganizationInvitationToProto(result.AutoAddEntry, nil)
	}

	return response, nil
}

// RemoveUserFromOrganizationGeneric RemoveUserFromOrganizationGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) RemoveUserFromOrganizationGeneric(r *http.Request, req *proto_models.RemoveUserFromOrganizationRequest) (*proto_models.RemoveUserFromOrganizationResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Call service layer with direct parameters
	err := h.serviceHandler.GetOrganizationHandler().RemoveMember(
		r.Context(),
		common_service_models.UserIdType(req.UserId),
		common_service_models.OrganizationIdType(req.OrganizationId),
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "remove_user_from_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId))
	}

	return &proto_models.RemoveUserFromOrganizationResponse{}, nil
}

// UpdateUserRoleInOrganizationGeneric UpdateUserRoleInOrganizationGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) UpdateUserRoleInOrganizationGeneric(r *http.Request, req *proto_models.UpdateUserRoleInOrganizationRequest) (*proto_models.UpdateUserRoleInOrganizationResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Call service layer with direct parameters
	memberResponse, err := h.serviceHandler.GetOrganizationHandler().UpdateMemberRole(
		r.Context(),
		common_service_models.UserIdType(req.UserId),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.Role,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "update_user_role_in_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId),
			zap.Stringer(appLogger.KeyNewRole, req.Role))
	}

	// Convert service response to proto response
	protoOrgUser := h.convertUserOrganizationToProto(memberResponse.UserOrganization)

	return &proto_models.UpdateUserRoleInOrganizationResponse{
		OrganizationUser: protoOrgUser,
	}, nil
}

// ListOrganizationUsersGeneric ListOrganizationUsersGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListOrganizationUsersGeneric(r *http.Request, req *proto_models.ListOrganizationUsersRequest) (*proto_models.ListOrganizationUsersResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated user ID (may be needed for authorization checks)
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Use sanitized pagination values directly (sanitizer sets defaults and validates limits)
	limit := int(req.Pagination.Limit)
	offset := int((req.Pagination.Page - 1) * req.Pagination.Limit)

	// Step 1: Get service models from service layer
	memberResponses, total, err := h.serviceHandler.GetOrganizationHandler().GetOrganizationMembers(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
		limit,
		offset,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_organization_users"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}

	// Step 2: Get permissions for viewing member attributes (PII protection)
	permissions, err := h.serviceHandler.GetOrganizationHandler().GetMemberViewPermissions(r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_member_view_permissions"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}

	// Step 3: Convert service models to proto models with permission-based filtering
	converter := h.serviceHandler.GetConverter()
	protoMembers := common_util.ApplyToEach(memberResponses, func(memberDetails *common_service_models.OrganizationMemberDetails) *proto_models.OrganizationMemberDetails {
		return converter.ServiceOrganizationMemberDetailsToFilteredProto(memberDetails, permissions)
	})

	// Create pagination response
	currentPage := uint32(offset/limit + 1)
	paginationResponse := common_util.CreatePaginationResponse(total, uint32(limit), currentPage)

	return &proto_models.ListOrganizationUsersResponse{
		Members:    protoMembers,
		Pagination: paginationResponse,
	}, nil
}

// CreateOrganizationEmailDomainGeneric CreateOrganizationEmailDomainGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) CreateOrganizationEmailDomainGeneric(r *http.Request, req *proto_models.CreateOrganizationEmailDomainRequest) (*proto_models.CreateOrganizationEmailDomainResponse, error) {
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	creatorID := userID
	serviceDomain, err := h.serviceHandler.GetOrganizationHandler().CreateOrganizationEmailDomain(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.Domain,
		&creatorID,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "create_organization_email_domain"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.String(appLogger.KeyDomain, req.Domain))
	}

	protoDomain := h.serviceHandler.GetConverter().ServiceOrganizationEmailDomainToProto(serviceDomain)

	return &proto_models.CreateOrganizationEmailDomainResponse{
		EmailDomain: protoDomain,
	}, nil
}

// DeleteOrganizationEmailDomainGeneric DeleteOrganizationEmailDomainGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) DeleteOrganizationEmailDomainGeneric(r *http.Request, req *proto_models.DeleteOrganizationEmailDomainRequest) (*proto_models.DeleteOrganizationEmailDomainResponse, error) {
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	err := h.serviceHandler.GetOrganizationHandler().DeleteOrganizationEmailDomain(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.OrganizationEmailDomainId,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "delete_organization_email_domain"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Uint64(appLogger.KeyEmailDomainID, req.OrganizationEmailDomainId))
	}

	return &proto_models.DeleteOrganizationEmailDomainResponse{}, nil
}

// ListOrganizationEmailDomainsGeneric ListOrganizationEmailDomainsGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListOrganizationEmailDomainsGeneric(r *http.Request, req *proto_models.ListOrganizationEmailDomainsRequest) (*proto_models.ListOrganizationEmailDomainsResponse, error) {
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	serviceDomains, pagination, err := h.serviceHandler.GetOrganizationHandler().ListOrganizationEmailDomainsPage(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.Pagination,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_organization_email_domains"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}

	protoDomains := common_util.ApplyToEach(serviceDomains, func(domain *common_service_models.OrganizationEmailDomain) *proto_models.OrganizationEmailDomain {
		return h.serviceHandler.GetConverter().ServiceOrganizationEmailDomainToProto(domain)
	})

	return &proto_models.ListOrganizationEmailDomainsResponse{
		EmailDomains: protoDomains,
		Pagination:   pagination,
	}, nil
}

// ListOrganizationsGeneric ListOrganizationsGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListOrganizationsGeneric(r *http.Request, req *proto_models.ListOrganizationsRequest) (*proto_models.ListOrganizationsResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Use sanitized pagination values directly (sanitizer sets defaults and validates limits)
	limit := int(req.Pagination.Limit)
	offset := int((req.Pagination.Page - 1) * req.Pagination.Limit)
	var parentOrganizationIDFilter *common_service_models.OrganizationIdType
	if req.ParentOrganizationIdFilter != nil {
		value := common_service_models.OrganizationIdType(req.GetParentOrganizationIdFilter())
		parentOrganizationIDFilter = &value
	}

	userOrgResponses, paginationResponse, err := h.serviceHandler.GetOrganizationHandler().ListUserOrganizationsWithRolesPage(
		r.Context(),
		userID,
		limit,
		offset,
		req.SortBy,
		req.Sort,
		req.Filter,
		req.OrganizationTypeFilter,
		parentOrganizationIDFilter,
		req.DepthFilter,
		common_util.ShouldIncludeTotalCount(req.Pagination),
		req.GetOrganizationMembershipMaskFilters(),
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_user_organizations"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Convert service responses to proto responses using converter
	protoOrgWithRoles := common_util.ApplyToEach(userOrgResponses, func(userOrgResponse *common_service_models.UserOrganizationResponse) *proto_models.OrganizationWithUserRole {
		// Use converter to transform service model to proto
		return h.serviceHandler.GetConverter().ServiceUserOrganizationResponseToOrganizationWithUserRole(userOrgResponse)
	})

	return &proto_models.ListOrganizationsResponse{
		Organizations: protoOrgWithRoles,
		Pagination:    paginationResponse,
	}, nil
}

// ListOrganizationsByParentGeneric ListOrganizationsByParentGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListOrganizationsByParentGeneric(r *http.Request, req *proto_models.ListOrganizationsByParentRequest) (*proto_models.ListOrganizationsByParentResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	limit := int(req.Pagination.Limit)
	offset := int((req.Pagination.Page - 1) * req.Pagination.Limit)

	var parentID *common_service_models.OrganizationIdType
	if req.ParentOrganizationId != nil {
		converted := common_service_models.OrganizationIdType(*req.ParentOrganizationId)
		parentID = &converted
	}

	orgResponses, total, err := h.serviceHandler.GetOrganizationHandler().ListOrganizationsByParent(r.Context(), parentID, limit, offset)
	if err != nil {
		fields := []zap.Field{
			zap.String(logkeys.Operation, "list_organizations_by_parent"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		}
		if parentID != nil {
			fields = append(fields, zap.Int64(appLogger.KeyOrganizationID, int64(*parentID)))
		}
		return nil, MapServiceErrorWithFields(err, fields...)
	}

	protoOrgs := make([]*proto_models.Organization, 0, len(orgResponses))
	for _, orgResponse := range orgResponses {
		if orgResponse == nil {
			continue
		}
		protoOrgs = append(protoOrgs, h.serviceHandler.GetConverter().ServiceOrganizationToProto(orgResponse.Organization))
	}

	currentPage := uint32(offset/limit + 1)
	paginationResponse := common_util.CreatePaginationResponse(total, uint32(limit), currentPage)

	return &proto_models.ListOrganizationsByParentResponse{
		Organizations: protoOrgs,
		Pagination:    paginationResponse,
	}, nil
}

// GetOrganizationHierarchyGeneric GetOrganizationHierarchyGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) GetOrganizationHierarchyGeneric(r *http.Request, req *proto_models.GetOrganizationHierarchyRequest) (*proto_models.GetOrganizationHierarchyResponse, error) {
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// The sanitizer already defaulted and bounded the page; read it as-is.
	page, limit := req.GetPagination().GetPage(), req.GetPagination().GetLimit()
	result, err := h.serviceHandler.GetOrganizationHandler().GetOrganizationHierarchyPage(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.IncludeDescendants,
		page,
		int(limit),
		common_util.PaginationOffset(page, limit),
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_organization_hierarchy"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}

	var buildHierarchy func(node *common_service_models.OrganizationHierarchyNode) *proto_models.OrganizationHierarchyNode
	buildHierarchy = func(node *common_service_models.OrganizationHierarchyNode) *proto_models.OrganizationHierarchyNode {
		if node == nil || node.Organization == nil {
			return nil
		}
		children := make([]*proto_models.OrganizationHierarchyNode, 0, len(node.Children))
		for _, child := range node.Children {
			children = append(children, buildHierarchy(child))
		}
		return &proto_models.OrganizationHierarchyNode{
			Organization: h.serviceHandler.GetConverter().ServiceOrganizationToProto(node.Organization),
			Children:     children,
		}
	}

	return &proto_models.GetOrganizationHierarchyResponse{
		Hierarchy:  buildHierarchy(result.Hierarchy),
		Pagination: result.Pagination,
		Truncated:  result.Truncated,
	}, nil
}

// HandleCheckOrganizationSlugAvailability HandleCheckOrganizationSlugAvailability handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) HandleCheckOrganizationSlugAvailability(r *http.Request, req *proto_models.CheckOrganizationSlugAvailabilityRequest) (*proto_models.CheckOrganizationSlugAvailabilityResponse, error) {
	ctx := r.Context()
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, commonAPILoggerName).Debug("checking organization slug availability",
			zap.String(appLogger.KeySlug, req.Slug))
	}

	available, message, err := h.serviceHandler.CheckOrganizationSlugAvailability(ctx, req.Slug)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "check_organization_slug_availability"),
			zap.String(appLogger.KeySlug, req.Slug))
	}

	// Create response
	response := &proto_models.CheckOrganizationSlugAvailabilityResponse{
		Available: available,
		Message:   message,
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, commonAPILoggerName).Debug("organization slug availability checked",
			zap.String(appLogger.KeySlug, req.Slug),
			zap.Bool(appLogger.KeyAvailable, available))
	}

	return response, nil
}

// Helper methods

// GetOrganizationMemberGeneric GetOrganizationMemberGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) GetOrganizationMemberGeneric(r *http.Request, req *proto_models.GetOrganizationMemberRequest) (*proto_models.GetOrganizationMemberResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Call service layer to get member details
	memberDetails, err := h.serviceHandler.GetOrganizationHandler().GetUserOrganizationMembership(r.Context(),
		common_service_models.UserIdType(req.UserId),
		common_service_models.OrganizationIdType(req.OrganizationId))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_organization_member"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId))
	}

	// Get permissions for viewing member attributes (PII protection)
	permissions, err := h.serviceHandler.GetOrganizationHandler().GetMemberViewPermissions(r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_member_view_permissions"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}

	// Convert service model to proto model with permission-based filtering
	protoMemberDetails := h.serviceHandler.GetConverter().ServiceOrganizationMemberDetailsToFilteredProto(memberDetails, permissions)

	return &proto_models.GetOrganizationMemberResponse{
		MemberDetails: protoMemberDetails,
	}, nil
}

// convertUserOrganizationToProto convertUserOrganizationToProto supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) convertUserOrganizationToProto(userOrg *common_service_models.UserOrganization) *proto_models.OrganizationUser {
	var inheritedFrom *uint64
	var effectiveRoleDepth *uint32
	if userOrg.InheritedFromOrganizationID != nil {
		converted := uint64(*userOrg.InheritedFromOrganizationID)
		inheritedFrom = &converted
	}
	if userOrg.EffectiveRoleDepth != nil {
		convertedDepth := *userOrg.EffectiveRoleDepth
		effectiveRoleDepth = &convertedDepth
	}
	return &proto_models.OrganizationUser{
		OrganizationId:              userOrg.OrganizationID,
		UserId:                      uint64(userOrg.UserID),
		ExplicitRole:                userOrg.ExplicitRole,
		EffectiveRole:               userOrg.EffectiveRole,
		CreatedAt:                   timestamppb.New(userOrg.CreatedAt),
		UpdatedAt:                   timestamppb.New(userOrg.UpdatedAt),
		InheritedFromOrganizationId: inheritedFrom,
		EffectiveRoleDepth:          effectiveRoleDepth,
	}
}

// InviteUserToOrganizationGeneric InviteUserToOrganizationGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) InviteUserToOrganizationGeneric(r *http.Request, req *proto_models.InviteUserToOrganizationRequest) (*proto_models.InviteUserToOrganizationResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	log := appLogger.For(r.Context(), h.logger, commonAPILoggerName).With(
		zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
		zap.Stringer(appLogger.KeyIdentifierType, req.IdentifierType),
	)

	// Call service layer
	invitationID, err := h.serviceHandler.GetOrganizationHandler().InviteUserToOrganization(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.IdentifierType,
		req.Identifier,
		req.Role,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "invite_user_to_organization"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Stringer(appLogger.KeyIdentifierType, req.IdentifierType),
			zap.String(appLogger.KeyIdentifier, req.Identifier),
			zap.Stringer(appLogger.KeyNewRole, req.Role))
	}

	log.Info("User invitation processed successfully",
		zap.String(logkeys.Operation, "invite_user_to_organization"),
		zap.Bool(appLogger.KeyUserAdded, false),
		zap.Bool(appLogger.KeyInvitationPending, true),
	)

	return &proto_models.InviteUserToOrganizationResponse{InvitationId: invitationID}, nil
}

// ListPendingInvitationsGeneric ListPendingInvitationsGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListPendingInvitationsGeneric(r *http.Request, req *proto_models.ListPendingInvitationsRequest) (*proto_models.ListPendingInvitationsResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Call service layer
	invitations, pagination, err := h.serviceHandler.GetOrganizationHandler().GetPendingInvitationsPage(r.Context(), req.Pagination)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_pending_invitations"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Convert to proto
	protoInvitations := common_util.ApplyToEach(invitations, func(invite *common_service_models.OrganizationInvitation) *proto_models.OrganizationInvitation {
		return h.serviceHandler.GetConverter().ServiceOrganizationInvitationToProto(invite, invite.Organization)
	})

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).With(
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		).Debug("Listed pending invitations",
			zap.String(logkeys.Operation, "list_pending_invitations"),
			zap.Int(appLogger.KeyCount, len(protoInvitations)))
	}

	return &proto_models.ListPendingInvitationsResponse{
		Invitations: protoInvitations,
		Pagination:  pagination,
	}, nil
}

// AcceptInvitationGeneric AcceptInvitationGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AcceptInvitationGeneric(r *http.Request, req *proto_models.AcceptInvitationRequest) (*proto_models.AcceptInvitationResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	log := appLogger.For(r.Context(), h.logger, commonAPILoggerName).With(
		zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		zap.Uint64(appLogger.KeyInvitationID, req.InvitationId),
	)

	// Call service layer
	membership, err := h.serviceHandler.GetOrganizationHandler().AcceptInvitation(r.Context(), req.InvitationId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "accept_invitation"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Uint64(appLogger.KeyInvitationID, req.InvitationId))
	}

	// Convert to proto
	protoOrgUser := h.convertUserOrganizationToProto(membership.UserOrganization)

	log.Info("Invitation accepted successfully",
		zap.String(logkeys.Operation, "accept_invitation"),
		zap.Uint64(appLogger.KeyOrganizationID, uint64(membership.OrganizationID)),
	)

	return &proto_models.AcceptInvitationResponse{
		OrganizationUser: protoOrgUser,
	}, nil
}

// DeclineInvitationGeneric DeclineInvitationGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) DeclineInvitationGeneric(r *http.Request, req *proto_models.DeclineInvitationRequest) (*proto_models.DeclineInvitationResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	log := appLogger.For(r.Context(), h.logger, commonAPILoggerName).With(
		zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		zap.Uint64(appLogger.KeyInvitationID, req.InvitationId),
	)

	// Call service layer
	err := h.serviceHandler.GetOrganizationHandler().DeclineInvitation(r.Context(), req.InvitationId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "decline_invitation"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Uint64(appLogger.KeyInvitationID, req.InvitationId))
	}

	log.Info("Invitation declined successfully", zap.String(logkeys.Operation, "decline_invitation"))

	return &proto_models.DeclineInvitationResponse{}, nil
}

// CancelInvitationGeneric CancelInvitationGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) CancelInvitationGeneric(r *http.Request, req *proto_models.CancelInvitationRequest) (*proto_models.CancelInvitationResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	log := appLogger.For(r.Context(), h.logger, commonAPILoggerName).With(
		zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
		zap.Uint64(appLogger.KeyInvitationID, req.InvitationId),
	)

	// Call service layer
	err := h.serviceHandler.GetOrganizationHandler().CancelInvitation(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.InvitationId,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "cancel_invitation"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Uint64(appLogger.KeyInvitationID, req.InvitationId))
	}

	log.Info("Invitation cancelled successfully", zap.String(logkeys.Operation, "cancel_invitation"))

	return &proto_models.CancelInvitationResponse{}, nil
}

// ListOrganizationInvitationsGeneric ListOrganizationInvitationsGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListOrganizationInvitationsGeneric(r *http.Request, req *proto_models.ListOrganizationInvitationsRequest) (*proto_models.ListOrganizationInvitationsResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Use sanitized pagination values directly (sanitizer sets defaults and validates limits)
	limit := int(req.Pagination.Limit)
	offset := int((req.Pagination.Page - 1) * req.Pagination.Limit)

	// Call service layer
	invitations, total, err := h.serviceHandler.GetOrganizationHandler().ListOrganizationInvitations(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
		limit,
		offset,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_organization_invitations"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}

	// Get the organization for conversion
	orgResponse, err := h.serviceHandler.GetOrganizationHandler().GetOrganization(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_organization_for_invitation_conversion"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}

	// Convert to proto
	protoInvitations := common_util.ApplyToEach(invitations, func(inv *common_service_models.OrganizationInvitation) *proto_models.OrganizationInvitation {
		return h.serviceHandler.GetConverter().ServiceOrganizationInvitationToProto(inv, orgResponse.Organization)
	})

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).With(
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
		).Debug("Listed organization invitations",
			zap.Int(appLogger.KeyCount, len(protoInvitations)),
			zap.Int64(appLogger.KeyTotal, total),
		)
	}

	// Create pagination response
	currentPage := uint32(offset/limit + 1)
	paginationResponse := common_util.CreatePaginationResponse(total, uint32(limit), currentPage)

	return &proto_models.ListOrganizationInvitationsResponse{
		Invitations: protoInvitations,
		Pagination:  paginationResponse,
	}, nil
}
