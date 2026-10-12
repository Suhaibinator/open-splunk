package ports

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// OrganizationRoleResolver resolves a user's EFFECTIVE role in an organization for packages
// that must authorize against organization membership without importing the organization
// service (for example the chat handler's org-derived channel read access).
//
// The effective role is lineage-aware: when organization role inheritance is enabled it is the
// highest of the user's explicit role in organizationID and the roles inherited from its ancestor
// organizations; otherwise it is the explicit role. proto_models.OrganizationRole is an ordered
// enum, so callers compare the result against a minimum-role threshold with `role >= threshold`.
//
// isMember is false when the user has no effective role in the organization, in which case role is
// ORGANIZATION_ROLE_UNSPECIFIED and the caller must grant no organization-derived access. A
// non-nil error means only that the lookup failed; callers must propagate it rather than treat it
// as "not a member" or as a grant.
//
// Implementations sit on hot authorization paths and should be cached. The default wiring adapts
// organization.OrgRoleProvider, whose process-local cache is invalidated on membership and role
// changes and bypassed inside an ambient database transaction.
type OrganizationRoleResolver interface {
	EffectiveOrganizationRole(
		ctx context.Context,
		userID common_service_models.UserIdType,
		organizationID common_service_models.OrganizationIdType,
	) (role proto_models.OrganizationRole, isMember bool, err error)
}

// FreshOrganizationRoleResolver bypasses cached authority when issuing a
// time-limited realtime grant. Implementations must read current persisted roles.
type FreshOrganizationRoleResolver interface {
	EffectiveOrganizationRoleFresh(context.Context, common_service_models.UserIdType, common_service_models.OrganizationIdType) (proto_models.OrganizationRole, bool, error)
}
