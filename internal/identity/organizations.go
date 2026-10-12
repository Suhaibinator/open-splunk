package identity

import (
	"context"
	"errors"
	"math"
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	gcapi "github.com/Suhaibinator/go-common/pkg/common_api"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	gerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
)

type rolePolicy struct{}

func (rolePolicy) ValidRole(role pb.OrganizationRole) bool {
	return authorization.ValidRole(int32(role))
}
func (rolePolicy) CanManageRole(actor, target pb.OrganizationRole) bool {
	return allowedRoleTransition(actor, target)
}
func (rolePolicy) CanGrantRole(actor, requested pb.OrganizationRole) bool {
	return allowedRoleTransition(actor, requested)
}
func allowedRoleTransition(actor, target pb.OrganizationRole) bool {
	if !authorization.ValidRole(int32(actor)) || !authorization.ValidRole(int32(target)) {
		return false
	}
	return int32(actor) == authorization.OwnerRole || int32(actor) == authorization.AdminRole && int32(target) != authorization.OwnerRole
}

func (r *Runtime) initialMemberships(ctx context.Context, input organization.OrganizationCreationInput) ([]organization.InitialMembership, error) {
	requested := input.RequestedMemberships
	if len(requested) != 1 || requested[0].UserID == 0 || int32(requested[0].Role) != authorization.OwnerRole {
		return nil, gerrors.ErrInvalidRequest
	}
	owner := requested[0]
	actor := authorization.Subject{UserID: input.ActorID, UserType: int32(input.ActorType)}
	if !actor.Valid() {
		return nil, gerrors.ErrInvalidCredentials
	}
	if input.Organization.ParentOrganizationID == nil {
		if !actor.IsSiteAdmin() {
			return nil, gerrors.ErrInsufficientPermissions
		}
		return []organization.InitialMembership{owner}, nil
	}
	parent := *input.Organization.ParentOrganizationID
	actorRole, member, err := r.EffectiveRole(ctx, input.ActorID, parent)
	if err != nil {
		return nil, err
	}
	if !actor.IsSiteAdmin() && (!member || actorRole < authorization.AdminRole) {
		return nil, gerrors.ErrInsufficientPermissions
	}
	ownerRole, ownerMember, err := r.EffectiveRole(ctx, owner.UserID, parent)
	if err != nil {
		return nil, err
	}
	if !ownerMember {
		return nil, gerrors.ErrInsufficientPermissions
	}
	// A parent Admin cannot elevate a peer into ownership while creating a child.
	// They may choose an existing parent Owner as the explicit initial child Owner.
	if !actor.IsSiteAdmin() && actorRole == authorization.AdminRole && ownerRole != authorization.OwnerRole {
		return nil, gerrors.ErrInsufficientPermissions
	}
	result := []organization.InitialMembership{owner}
	if member && actorRole >= authorization.AdminRole && input.ActorID != owner.UserID {
		result = append(result, organization.InitialMembership{UserID: input.ActorID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0})
	}
	return result, nil
}

// CreateOrganizationRequest extends the shared create workflow only with an
// explicit initial owner. OAuth/session/member mutation routes remain GoCommon.
type CreateOrganizationRequest struct {
	Name                 string  `json:"name"`
	Slug                 string  `json:"slug"`
	Description          *string `json:"description,omitempty"`
	ParentOrganizationID *int64  `json:"parent_organization_id,omitempty"`
	OwnerUserID          uint64  `json:"owner_user_id"`
}
type CreateOrganizationResponse struct {
	OrganizationID       int64  `json:"organization_id"`
	Name                 string `json:"name"`
	Slug                 string `json:"slug"`
	ParentOrganizationID *int64 `json:"parent_organization_id,omitempty"`
}

func (r *Runtime) registerCreationRoute() {
	group := r.router.Group(Prefix).AuthToken(&common.AuthTokenConfig{Source: common.AuthTokenSourceCookie, CookieName: r.cookieName})
	group.Route(router.RouteConfig[*CreateOrganizationRequest, *CreateOrganizationResponse]{
		Path: "/organizations/create-with-owner", Methods: []router.HttpMethod{http.MethodPost, http.MethodOptions}, AuthLevel: new(router.AuthRequired),
		Codec: codec.NewJSONCodec[*CreateOrganizationRequest, *CreateOrganizationResponse](), SourceType: router.Body,
		Middlewares: []common.Middleware{r.api.GetTransactionMiddleware()},
		Sanitizer: func(ctx context.Context, req *CreateOrganizationRequest) (*CreateOrganizationRequest, error) {
			if req == nil || req.OwnerUserID == 0 || req.OwnerUserID > math.MaxInt64 {
				return nil, authorization.ErrInvalidPolicy
			}
			var parent *uint64
			if req.ParentOrganizationID != nil {
				if *req.ParentOrganizationID <= 0 {
					return nil, authorization.ErrInvalidPolicy
				}
				parent = new(uint64(*req.ParentOrganizationID))
			}
			sanitized, err := r.sanitizer.SanitizeCreateOrganizationRequest(ctx, &pb.CreateOrganizationRequest{Name: req.Name, Slug: req.Slug, Description: req.Description, ParentOrganizationId: parent})
			if err != nil {
				return nil, err
			}
			req.Name, req.Slug, req.Description = sanitized.Name, sanitized.Slug, sanitized.Description
			return req, nil
		},
		Handler: func(req *http.Request, input *CreateOrganizationRequest) (*CreateOrganizationResponse, error) {
			creator := r.service.GetOrganizationHandler().(organization.InitialMembershipCreator)
			org := &sm.Organization{Name: input.Name, Slug: input.Slug, ParentOrganizationID: input.ParentOrganizationID}
			if input.Description != nil {
				org.Description = *input.Description
			}
			response, err := creator.CreateOrganizationWithInitialMemberships(req.Context(), org, []organization.InitialMembership{{UserID: input.OwnerUserID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}})
			if err != nil {
				if errors.Is(err, authorization.ErrForbidden) || errors.Is(err, authorization.ErrUnauthorized) {
					return nil, router.NewHTTPErrorWithCause(http.StatusForbidden, "Organization creation denied", err)
				}
				if errors.Is(err, authorization.ErrInvalidPolicy) {
					return nil, router.NewHTTPErrorWithCause(http.StatusBadRequest, "An explicit initial owner is required", err)
				}
				return nil, gcapi.MapServiceError(err)
			}
			return &CreateOrganizationResponse{OrganizationID: response.ID, Name: response.Name, Slug: response.Slug, ParentOrganizationID: response.ParentOrganizationID}, nil
		},
	})
}
