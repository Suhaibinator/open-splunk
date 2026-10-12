package organization

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// MembershipRolePolicy refines role transitions after configured operation
// permissions have passed. It never replaces membership, ancestry, or last-owner
// checks. A nil policy preserves the strict ordinal role hierarchy.
type MembershipRolePolicy interface {
	CanManageRole(actor, target proto_models.OrganizationRole) bool
	CanGrantRole(actor, requested proto_models.OrganizationRole) bool
}

// MembershipRoleValidator optionally restricts the application's exposed role
// vocabulary, including trusted/site-admin mutations.
type MembershipRoleValidator interface {
	ValidRole(proto_models.OrganizationRole) bool
}

func (h *OrganizationHandler[U]) validateApplicationRole(role proto_models.OrganizationRole) error {
	if validator, ok := h.applicationPolicy.roles.(MembershipRoleValidator); ok && !validator.ValidRole(role) {
		return errors.ErrInvalidRequest
	}
	return nil
}

type InitialMembership struct {
	UserID common_service_models.UserIdType
	Role   proto_models.OrganizationRole
}

// OrganizationCreationInput contains the authenticated actor and validated
// organization. RequestedMemberships is application input, not authorization.
type OrganizationCreationInput struct {
	ActorID              common_service_models.UserIdType
	ActorType            proto_models.UserType
	Organization         *common_service_models.Organization
	CreatorRole          proto_models.OrganizationRole
	AutoAddCreator       bool
	RequestedMemberships []InitialMembership
}

// InitialMembershipPolicy selects and authorizes initial memberships before any
// organization write. Importing applications can require an explicit owner and
// retain an Admin creator as Admin. GoCommon still enforces ancestor membership.
type InitialMembershipPolicy func(context.Context, OrganizationCreationInput) ([]InitialMembership, error)

// InitialMembershipCreator is the additive application creation surface. Native
// routes continue to use the same policy with no requested initial memberships.
type InitialMembershipCreator interface {
	CreateOrganizationWithInitialMemberships(context.Context, *common_service_models.Organization, []InitialMembership) (*OrganizationResponse, error)
}

type HandlerOption func(*applicationPolicy)

type applicationPolicy struct {
	roles              MembershipRolePolicy
	initialMemberships InitialMembershipPolicy
}

func WithMembershipRolePolicy(policy MembershipRolePolicy) HandlerOption {
	return func(options *applicationPolicy) { options.roles = policy }
}

func WithInitialMembershipPolicy(policy InitialMembershipPolicy) HandlerOption {
	return func(options *applicationPolicy) { options.initialMemberships = policy }
}

func (h *OrganizationHandler[U]) canManageRole(actor, target proto_models.OrganizationRole) bool {
	if h.applicationPolicy.roles != nil {
		return h.applicationPolicy.roles.CanManageRole(actor, target)
	}
	return common_service_models.CanManageRole(actor, target)
}

func (h *OrganizationHandler[U]) canGrantRole(actor, requested proto_models.OrganizationRole) bool {
	if h.applicationPolicy.roles != nil {
		return h.applicationPolicy.roles.CanGrantRole(actor, requested)
	}
	return common_service_models.CanPromoteToRole(actor, requested)
}

// CreateOrganizationWithInitialMemberships uses the ordinary create permissions
// and the configured initial-membership policy in one caller-owned transaction.
func (h *OrganizationHandler[U]) CreateOrganizationWithInitialMemberships(ctx context.Context, org *common_service_models.Organization, members []InitialMembership) (*OrganizationResponse, error) {
	if h.applicationPolicy.initialMemberships == nil {
		return nil, errors.ErrInvalidRequest
	}
	return h.createOrganizationWithMode(ctx, org, organizationCreateModeUser, members)
}
