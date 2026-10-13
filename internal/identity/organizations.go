package identity

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"net/http"
	"slices"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	gcapi "github.com/Suhaibinator/go-common/pkg/common_api"
	gcpersistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	gerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
)

// InitialMembership is OpenSplunk's explicit initial-ownership request. Native
// GoCommon creation and privileged membership APIs publish it in one transaction.
type InitialMembership struct {
	UserID sm.UserIdType
	Role   pb.OrganizationRole
}

type OrganizationCreationInput struct {
	ActorID              sm.UserIdType
	ActorType            pb.UserType
	Organization         *sm.Organization
	CreatorRole          pb.OrganizationRole
	AutoAddCreator       bool
	RequestedMemberships []InitialMembership
}

func (r *Runtime) initialMemberships(ctx context.Context, input OrganizationCreationInput) ([]InitialMembership, error) {
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
		return []InitialMembership{owner}, nil
	}
	parent := *input.Organization.ParentOrganizationID
	// The composer loads GoCommon's canonical parent lineage. Lock every
	// ancestor before inserting the child: physical foreign keys alone do not
	// exclude a parent whose soft deletion committed while creation was waiting.
	ambient, ok := scontext.GetTransaction(ctx)
	if !ok || ambient == nil || ambient.GetDB() == nil || ambient.GetDB().Statement == nil {
		return nil, gerrors.ErrTransactionNotFound
	}
	tx, ok := ambient.GetDB().Statement.ConnPool.(*sql.Tx)
	if !ok {
		return nil, gerrors.ErrTransactionNotFound
	}
	// A child can initialize both its Owner and the acting parent Admin. Lock
	// every initial user's lifetime before any insertion acquires the hierarchy
	// guard; user deletion takes that same user-first order.
	users := []uint64{input.ActorID, owner.UserID}
	slices.Sort(users)
	for _, userID := range slices.Compact(users) {
		var live uint64
		var userType int32
		if err := tx.QueryRowContext(ctx, "SELECT id,user_type FROM gocommon_users WHERE id=$1 AND deleted_at IS NULL FOR SHARE", userID).Scan(&live, &userType); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, gerrors.ErrUserNotFound
			}
			return nil, err
		}
		if userID == input.ActorID {
			actor.UserType = userType
			if !actor.Valid() {
				return nil, gerrors.ErrInvalidCredentials
			}
		}
	}
	ancestors := append([]int64(nil), input.Organization.GetLineage()...)
	ancestors = append(ancestors, parent)
	// Canonical lineage order is root-to-parent, independent of imported IDs.
	// Subtree deletion locks shallower organizations before their descendants.
	seen := make(map[int64]bool, len(ancestors))
	for _, ancestor := range ancestors {
		if seen[ancestor] {
			continue
		}
		seen[ancestor] = true
		var live int64
		if err := tx.QueryRowContext(ctx, "SELECT id FROM gocommon_organizations WHERE id=$1 AND deleted_at IS NULL FOR SHARE", ancestor).Scan(&live); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, gerrors.ErrOrganizationNotFound
			}
			return nil, err
		}
	}
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
	result := []InitialMembership{owner}
	if member && actorRole >= authorization.AdminRole && input.ActorID != owner.UserID {
		result = append(result, InitialMembership{UserID: input.ActorID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0})
	}
	return result, nil
}

// CreateOrganizationWithInitialMemberships is the application creation boundary.
// It joins the native transaction and uses GoCommon's ordinary organization and
// privileged member APIs after checking OpenSplunk's explicit-owner policy.
func (r *Runtime) CreateOrganizationWithInitialMemberships(ctx context.Context, org *sm.Organization, requested []InitialMembership) (*organization.OrganizationResponse, error) {
	if org == nil || len(requested) != 1 || requested[0].UserID == 0 || requested[0].UserID > math.MaxInt64 || int32(requested[0].Role) != authorization.OwnerRole {
		return nil, gerrors.ErrInvalidRequest
	}
	if org.PersonalOwnerUserID != nil || (org.OrganizationType != pb.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED && org.OrganizationType != TeamOrganizationType) {
		return nil, gerrors.ErrInvalidRequest
	}
	ambient, ok := scontext.GetTransaction(ctx)
	if !ok || ambient == nil || ambient.GetDB() == nil || ambient.GetDB().Statement == nil {
		return nil, gerrors.ErrTransactionNotFound
	}
	tx, ok := ambient.GetDB().Statement.ConnPool.(*sql.Tx)
	if !ok {
		return nil, gerrors.ErrTransactionNotFound
	}
	if _, hooksPresent := gcpersistence.DatabasePostCommitHookRegistrarFromContext(ctx); !hooksPresent {
		return nil, gerrors.ErrTransactionNotFound
	}
	user, ok := scontext.GetUser[sm.UserIdType, sm.User](ctx)
	if !ok || user == nil || user.ID == 0 {
		return nil, gerrors.ErrUserNotFound
	}
	ids := []uint64{user.ID, requested[0].UserID}
	slices.Sort(ids)
	for _, id := range slices.Compact(ids) {
		var live uint64
		if err := tx.QueryRowContext(ctx, "SELECT id FROM gocommon_users WHERE id=$1 AND deleted_at IS NULL FOR SHARE", id).Scan(&live); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, gerrors.ErrUserNotFound
			}
			return nil, err
		}
	}
	actor, err := r.CurrentSubjectInTransaction(ctx, user.ID)
	if err != nil {
		return nil, err
	}
	fresh := *user
	fresh.UserType = pb.UserType(actor.UserType)
	ctx = scontext.WithUser[sm.UserIdType](ctx, &fresh)
	org.OrganizationType = TeamOrganizationType
	if org.ParentOrganizationID != nil {
		parent, err := r.persistence.GetOrganizationByID(ctx, *org.ParentOrganizationID)
		if err != nil {
			if errors.Is(err, gcpersistence.ErrOrganizationNotFound) {
				return nil, gerrors.ErrOrganizationNotFound
			}
			return nil, err
		}
		if parent.PersonalOwnerUserID != nil || parent.OrganizationType != TeamOrganizationType {
			return nil, gerrors.ErrInsufficientPermissions
		}
		org.Lineage = append(append([]int64(nil), parent.Lineage...), parent.ID)
	}
	members, err := r.initialMemberships(ctx, OrganizationCreationInput{ActorID: user.ID, ActorType: fresh.UserType, Organization: org, RequestedMemberships: requested})
	if err != nil {
		return nil, err
	}
	handler := r.service.GetOrganizationHandler()
	privileged, ok := handler.(organization.OrganizationPrivilegedHandlerInterface)
	if !ok {
		return nil, gerrors.ErrInternalError
	}
	var response *organization.OrganizationResponse
	if org.ParentOrganizationID == nil {
		response, err = handler.CreateOrganizationByAdmin(ctx, org)
	} else {
		response, err = handler.CreateOrganizationByUser(ctx, org)
	}
	if err != nil {
		return nil, err
	}
	for _, member := range members {
		if _, err := privileged.AddMemberPrivileged(ctx, member.UserID, response.ID, member.Role); err != nil {
			return nil, err
		}
	}
	response.MemberCount = int64(len(members))
	return response, nil
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
			org := &sm.Organization{Name: input.Name, Slug: input.Slug, ParentOrganizationID: input.ParentOrganizationID}
			if input.Description != nil {
				org.Description = *input.Description
			}
			response, err := r.CreateOrganizationWithInitialMemberships(req.Context(), org, []InitialMembership{{UserID: input.OwnerUserID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}})
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
