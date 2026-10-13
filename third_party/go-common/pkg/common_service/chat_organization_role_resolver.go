package common_service

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// orgRoleProviderResolver adapts the shared organization.OrgRoleProvider to
// ports.OrganizationRoleResolver, so the chat handler can evaluate a channel's org-derived access
// against the same lineage-aware, cached role lookups the organization service uses without
// importing that package. Membership and role changes already invalidate the provider's cache, so
// a revoked role stops granting channel reads as soon as the organization service observes it.
type orgRoleProviderResolver struct {
	provider *organization.OrgRoleProvider
}

var _ ports.OrganizationRoleResolver = orgRoleProviderResolver{}

// newOrgRoleProviderResolver returns nil for a nil provider so the caller leaves the chat
// handler's resolver unset, which grants no org-derived access.
func newOrgRoleProviderResolver(provider *organization.OrgRoleProvider) ports.OrganizationRoleResolver {
	if provider == nil {
		return nil
	}
	return orgRoleProviderResolver{provider: provider}
}

// EffectiveOrganizationRole reports OrgRoleInfo.EffectiveRole, never the explicit role: inherited
// authority from ancestor organizations counts toward a channel's minimum-role thresholds.
func (r orgRoleProviderResolver) EffectiveOrganizationRole(
	ctx context.Context,
	userID common_service_models.UserIdType,
	organizationID common_service_models.OrganizationIdType,
) (proto_models.OrganizationRole, bool, error) {
	info, isMember, err := r.provider.GetUserRole(ctx, userID, organizationID)
	if err != nil {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, false, err
	}
	if !isMember {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, false, nil
	}
	return info.EffectiveRole, true, nil
}

func (r orgRoleProviderResolver) EffectiveOrganizationRoleFresh(ctx context.Context, userID common_service_models.UserIdType, organizationID common_service_models.OrganizationIdType) (proto_models.OrganizationRole, bool, error) {
	info, member, err := r.provider.GetUserRoleFresh(ctx, userID, organizationID)
	return info.EffectiveRole, member, err
}
