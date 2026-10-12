package organization

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// getViewerRole resolves the requesting user's effective role in the organization before member-attribute filtering is computed.
//
// Parameters:
//   - ctx: Request context used to resolve the authenticated user and membership.
//   - orgID: Organization whose viewer role should be determined.
//
// Returns:
//   - proto_models.OrganizationRole: Effective viewer role for the authenticated user.
//   - error: User-resolution or membership lookup error.
func (h *OrganizationHandler[U]) getViewerRole(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
) (proto_models.OrganizationRole, error) {
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, errors.ErrUserNotFound
	}
	requestingUserID := (*requestingUser).GetID()

	viewerRole, isMember, err := h.GetUserRoleInOrganization(ctx, requestingUserID, orgID)
	if err != nil {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, err
	}
	if !isMember {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, nil
	}

	return viewerRole, nil
}

// attributePermissionPolicy samples the hot attribute-visibility provider once
// and returns that generation's lookup tables, compiled once per
// configuration generation.
func (h *OrganizationHandler[U]) attributePermissionPolicy() attributePermissionTables {
	if h.attributeTables == nil {
		return attributePermissionTables{}
	}
	return h.attributeTables()
}

// GetMemberViewPermissions computes the permissions for viewing organization
// member details from the requesting user's organization role and site user
// type, using the attribute-visibility generation current at the time of the
// call. The API layer applies the result when converting member rows; nil
// means no policy is configured and nothing is filtered.
func (h *OrganizationHandler[U]) GetMemberViewPermissions(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
) (*config.UserAttributePermissions, error) {
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, errors.ErrUserNotFound
	}

	viewerRole, err := h.getViewerRole(ctx, orgID)
	if err != nil {
		return nil, err
	}

	orgDepth, err := h.getOrgDepth(ctx, orgID)
	if err != nil {
		return nil, err
	}

	// One provider sample per operation: the union below reads one coherent
	// generation even if a new one is published mid-request.
	policy := h.attributePermissionPolicy()
	return policy.unionPermissions(viewerRole, (*requestingUser).GetUserType(), orgDepth), nil
}

// Note: Pure filtering logic lives in the converter package:
// - FilterOrganizationMemberDetailsWithPermissions()
// - FilterProtoUserWithPermissions()
// - ConvertUserLikeToProtoWithPermissions()
//
// This file contains only organization-specific permission resolution.
