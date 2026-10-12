package organization

import (
	"context"
	stdErrors "errors"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// HasOrganizationPermission checks whether a user has the required role or higher within an organization during service-layer authorization.
//
// Parameters:
//   - ctx: Request context used for logging and role resolution.
//   - userID: User whose permission should be checked.
//   - orgID: Organization where permission is evaluated.
//   - requiredRole: Minimum role required for the operation.
//
// Returns:
//   - bool: `true` when the user satisfies the required role threshold.
//   - error: Organization lookup or internal role-resolution error.
func (h *OrganizationHandler[U]) HasOrganizationPermission(
	ctx context.Context,
	userID common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
	requiredRole proto_models.OrganizationRole,
) (bool, error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.Stringer(appLogger.KeyRequiredRole, requiredRole),
		).Debug("HasOrganizationPermission called")
	}

	if userID == 0 || orgID == 0 {
		return false, nil
	}

	// Get user's role in the organization
	userRole, err := h.getUserRoleInOrganization(ctx, userID, orgID)
	if err != nil {
		if stdErrors.Is(err, errors.ErrUserNotMember) || stdErrors.Is(err, persistence.ErrUserOrganizationNotFound) {
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, organizationLoggerName).With(
					zap.Uint64(appLogger.KeyUserID, uint64(userID)),
					zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
					zap.Stringer(appLogger.KeyRequiredRole, requiredRole),
				).Debug("user is not a member of the organization")
			}
			return false, nil // User is not a member, so no permissions
		}
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, organizationLoggerName).With(
					zap.Uint64(appLogger.KeyUserID, uint64(userID)),
					zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
					zap.Stringer(appLogger.KeyRequiredRole, requiredRole),
				).Debug("organization not found during permission check")
			}
			return false, err // Propagate organization not found error
		}
		return false, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "check_organization_permission"),
			zap.String(logkeys.Stage, "resolve_user_role"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.Stringer(appLogger.KeyRequiredRole, requiredRole),
		)
	}

	hasPermission := userRole >= requiredRole
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.Stringer(appLogger.KeyRequiredRole, requiredRole),
		).Debug("permission check result",
			zap.Stringer(appLogger.KeyUserRole, userRole),
			zap.Stringer(appLogger.KeyRequiredRole, requiredRole),
			zap.Bool(appLogger.KeyHasPermission, hasPermission))
	}

	return hasPermission, nil
}

// getActorOrgMembership resolves actor user type and effective organization role for permission checks that combine global and organization-scoped authorization.
//
// Parameters:
//   - ctx: Request context used for role-provider lookups.
//   - actor: Actor user model performing the operation.
//   - orgID: Organization context for membership resolution.
//
// Returns:
//   - proto_models.UserType: Actor global user type.
//   - proto_models.OrganizationRole: Actor effective organization role.
//   - bool: Membership flag indicating whether the actor belongs to the organization.
//   - error: Internal or lookup error encountered while resolving membership state.
func (h *OrganizationHandler[U]) getActorOrgMembership(
	ctx context.Context,
	actor common_service_models.UserLike,
	orgID common_service_models.OrganizationIdType,
) (proto_models.UserType, proto_models.OrganizationRole, bool, error) {
	if actor == nil {
		return proto_models.UserType_USER_TYPE_UNSPECIFIED,
			proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED,
			false,
			appLogger.InvariantError(
				"organization_permission_actor_non_nil",
				errors.ErrInternalError,
				zap.String(logkeys.Operation, "resolve_actor_organization_membership"),
				zap.String(logkeys.Stage, "validate_actor"),
				zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
				zap.String(logkeys.Expected, "non-nil actor"),
				zap.String(logkeys.Actual, "nil actor"),
			)
	}

	userType := actor.GetUserType()
	if orgID == 0 {
		return userType, proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, false, nil
	}

	if h.orgRoleProvider == nil {
		return userType, proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, false,
			appLogger.InvariantError(
				"organization_role_provider_initialized",
				errors.ErrInternalError,
				zap.String(logkeys.Operation, "resolve_actor_organization_membership"),
				zap.String(logkeys.Stage, "resolve_role"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actor.GetID())),
				zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
				zap.String(logkeys.Expected, "organization role provider configured"),
				zap.String(logkeys.Actual, "nil organization role provider"),
			)
	}

	roleInfo, isMember, err := h.orgRoleProvider.GetUserRole(ctx, actor.GetID(), orgID)
	if err != nil {
		return userType, proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, false, err
	}

	return userType, roleInfo.EffectiveRole, isMember, nil
}

// authorizeOrganizationMemberRead enforces the shared access policy for
// listing organization members and reading an individual member's details.
func (h *OrganizationHandler[U]) authorizeOrganizationMemberRead(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
	orgDepth int,
) (common_service_models.UserIdType, error) {
	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Int64(appLogger.KeyOrganizationID, orgID),
	)

	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return 0, errors.ErrUserNotFound
	}
	actorID := (*requestingUser).GetID()
	log = log.With(zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)))

	if h.permissionHandler == nil {
		return 0, appLogger.InvariantError(
			"organization_permission_handler_initialized",
			errors.ErrInternalError,
			zap.String(logkeys.Operation, "authorize_organization_member_read"),
			zap.String(logkeys.Stage, "permission_check"),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.String(logkeys.Expected, "permission handler configured"),
			zap.String(logkeys.Actual, "nil permission handler"),
		)
	}

	actorType, actorRole, _, err := h.getActorOrgMembership(ctx, *requestingUser, orgID)
	if err != nil {
		return 0, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "authorize_organization_member_read"),
			zap.String(logkeys.Stage, "resolve_actor_membership"),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
		)
	}

	canView, err := h.permissionHandler.HasOrgPermission(
		ctx,
		actorType,
		actorRole,
		permission.PermissionOrganizationListMembers,
		orgDepth,
	)
	if err != nil {
		return 0, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "authorize_organization_member_read"),
			zap.String(logkeys.Stage, "permission_check"),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
		)
	}
	if !canView {
		return 0, appLogger.WithErrorFields(errors.ErrInsufficientPermissions,
			zap.String(logkeys.Operation, "authorize_organization_member_read"),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.String(logkeys.Reason, "insufficient_permission"),
		)
	}

	return actorID, nil
}

// IsSlugAvailable checks if an organization slug is available for use.
// The excludeOrgID parameter allows checking availability while excluding a specific organization
// (useful for updates where the organization keeps the same slug).
//
// Parameters:
//   - ctx: Request context used for logging and persistence.
//   - slug: Slug to validate and test.
//   - excludeOrgID: Optional organization ID to exclude from availability checks.
//
// Returns:
//   - bool: `true` when the slug is available.
//   - error: Persistence or internal error while checking availability.
func (h *OrganizationHandler[U]) IsSlugAvailable(
	ctx context.Context,
	slug string,
	excludeOrgID ...common_service_models.OrganizationIdType,
) (bool, error) {
	slug = strings.ToLower(strings.TrimSpace(slug))
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.String(appLogger.KeySlug, slug),
		).Debug("IsSlugAvailable called")
	}
	if h.isReservedPersonalWorkspaceSlug(slug) {
		return false, errors.ErrOrganizationSlugReserved
	}

	if !h.isValidSlug(slug) {
		return false, nil
	}

	// Try to get organization by slug
	existingOrg, err := h.persistenceHandler.GetOrganizationBySlug(ctx, slug)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			// Slug is available
			return true, nil
		}
		return false, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "check_organization_slug_availability"),
			zap.String(logkeys.Stage, "lookup_organization_by_slug"),
			zap.String(appLogger.KeySlug, slug),
		)
	}

	// If we have an existing organization with this slug, check if we should exclude it
	if len(excludeOrgID) > 0 && existingOrg.ID == excludeOrgID[0] {
		// The slug belongs to the excluded organization, so it's available for that org
		return true, nil
	}

	// Slug is taken by another organization
	return false, nil
}

// CanManageEmailDomains checks whether the current user can manage organization email domains.
//
// Parameters:
//   - ctx: Request context used for actor resolution and authorization.
//   - orgID: Organization whose email-domain management permission should be checked.
//
// Returns:
//   - bool: `true` when the current user may manage email domains for the organization.
//   - error: Validation, organization-resolution, or authorization error.
func (h *OrganizationHandler[U]) CanManageEmailDomains(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
) (bool, error) {
	runtimeConfig := h.runtimeConfig()
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
		).Debug("CanManageEmailDomains called")
	}

	if orgID == 0 {
		return false, nil
	}
	if err := h.ensureOrganizationTypeCapability(ctx, orgID, organizationTypeCapabilityManageEmailDomains); err != nil {
		if stdErrors.Is(err, errors.ErrOrganizationNotFound) {
			return false, errors.ErrOrganizationNotFound
		}
		if stdErrors.Is(err, errors.ErrInvalidOrganizationID) {
			return false, errors.ErrInvalidOrganizationID
		}
		if stdErrors.Is(err, errors.ErrOrganizationTypeCapabilityDenied) {
			return false, nil
		}
		return false, errors.ErrInternalError
	}

	if !h.runtimeConfig().EnableOrganizationEmailDomains() {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, organizationLoggerName).With(
				zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			).Debug("organization email domains feature disabled")
		}
		return false, nil
	}

	user, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return false, appLogger.InvariantError(
			"organization_permission_actor_in_context",
			errors.ErrInternalError,
			zap.String(logkeys.Operation, "manage_organization_email_domains"),
			zap.String(logkeys.Stage, "resolve_actor"),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.String(logkeys.Expected, "authenticated actor in context"),
			zap.String(logkeys.Actual, "actor absent"),
		)
	}
	actorType, actorRole, _, err := h.getActorOrgMembership(ctx, *user, orgID)
	if err != nil {
		return false, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "manage_organization_email_domains"),
			zap.String(logkeys.Stage, "resolve_actor_membership"),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*user).GetID())),
		)
	}
	orgDepth, depthErr := h.getOrgDepthMapped(ctx, orgID)
	if depthErr != nil {
		return false, depthErr
	}

	allowOrgRole := runtimeConfig.AllowOrganizationManagedEmailDomains()
	if !allowOrgRole {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, organizationLoggerName).With(
				zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			).Debug("organization-managed email domains disabled, using site-level admin check only")
		}
		// Only site admins can manage - no org-role authorization
		return h.permissionHandler.HasOrgPermission(ctx, actorType, proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, permission.PermissionOrganizationManageEmailDomains, 0)
	}

	return h.permissionHandler.HasOrgPermission(
		ctx,
		actorType, actorRole,
		permission.PermissionOrganizationManageEmailDomains,
		orgDepth,
	)
}

// CanDeleteOrganization checks if a user can delete an organization.
func (h *OrganizationHandler[U]) CanDeleteOrganization(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
) (bool, error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
		).Debug("CanDeleteOrganization called")
	}

	if orgID == 0 {
		return false, nil
	}

	user, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return false, appLogger.InvariantError(
			"organization_permission_actor_in_context",
			errors.ErrInternalError,
			zap.String(logkeys.Operation, "delete_organization"),
			zap.String(logkeys.Stage, "resolve_actor"),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.String(logkeys.Expected, "authenticated actor in context"),
			zap.String(logkeys.Actual, "actor absent"),
		)
	}
	actorType, actorRole, _, err := h.getActorOrgMembership(ctx, *user, orgID)
	if err != nil {
		return false, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "delete_organization"),
			zap.String(logkeys.Stage, "resolve_actor_membership"),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*user).GetID())),
		)
	}
	orgDepth, depthErr := h.getOrgDepthMapped(ctx, orgID)
	if depthErr != nil {
		return false, depthErr
	}

	return h.permissionHandler.HasOrgPermission(
		ctx,
		actorType, actorRole,
		permission.PermissionOrganizationDelete,
		orgDepth,
	)
}

// GetUserRoleInOrganization gets a user's role within an organization.
func (h *OrganizationHandler[U]) GetUserRoleInOrganization(
	ctx context.Context,
	userID common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
) (proto_models.OrganizationRole, bool, error) {
	if userID == 0 || orgID == 0 {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, false, nil
	}

	userRole, err := h.getUserRoleInOrganization(ctx, userID, orgID)
	if err != nil {
		if stdErrors.Is(err, errors.ErrUserNotMember) {
			return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, false, nil
		}
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, false, appLogger.WithErrorCause(
			errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "get_user_organization_role"),
			zap.String(logkeys.Stage, "resolve_user_role"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
		)
	}

	return userRole, true, nil
}

// GetUserRoleInfo returns the user's effective role info using the cached role provider.
//
// Parameters:
//   - ctx: Request context used for role resolution.
//   - userID: User whose role information should be resolved.
//   - orgID: Organization where membership is evaluated.
//
// Returns:
//   - OrgRoleInfo: Explicit and effective role information.
//   - bool: `true` when the user is a member of the organization.
//   - error: Internal role-resolution error.
func (h *OrganizationHandler[U]) GetUserRoleInfo(
	ctx context.Context,
	userID common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
) (OrgRoleInfo, bool, error) {
	if h.orgRoleProvider == nil {
		return OrgRoleInfo{EffectiveRole: proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED}, false,
			appLogger.InvariantError(
				"organization_role_provider_initialized",
				errors.ErrInternalError,
				zap.String(logkeys.Operation, "get_user_organization_role_info"),
				zap.String(logkeys.Stage, "resolve_role"),
				zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
				zap.String(logkeys.Expected, "organization role provider configured"),
				zap.String(logkeys.Actual, "nil organization role provider"),
			)
	}

	return h.orgRoleProvider.GetUserRole(ctx, userID, orgID)
}

// ValidateRoleTransition checks if a role transition is valid according to organizational hierarchy rules.
//
// Security Rules:
//   - Actor's role must be strictly higher than the target's current role (CanManageRole)
//   - Actor's role must be strictly higher than the new role (CanPromoteToRole)
//   - ORGANIZATION_ROLE_OWNER_TIER_10 is exempt from both rules
//   - Prevents privilege escalation attacks through role manipulation
//
// Role Hierarchy: OrganizationRole is one ordered enum, compared numerically across
// families and tiers: MEMBER_TIER_0..10 < MODERATOR_TIER_0..10 < ADMIN_TIER_0..10 <
// OWNER_TIER_0..10. Equal is not higher, so a role can neither manage nor grant its own tier.
//
// Examples:
//   - ADMIN_TIER_1 can demote ADMIN_TIER_0 to MEMBER_TIER_1 ✓
//   - ADMIN_TIER_1 cannot manage another ADMIN_TIER_1 ✗ (ErrCannotManageHigherRole)
//   - MODERATOR_TIER_1 cannot promote MEMBER_TIER_1 to ADMIN_TIER_0 ✗ (ErrCannotPromoteAboveOwnRole)
//   - MEMBER_TIER_1 cannot manage ADMIN_TIER_0 ✗ (ErrCannotManageHigherRole)
//
// Parameters:
//   - actorRole: Role of the user performing the transition
//   - currentRole: Current role of the target user
//   - newRole: Desired new role for the target user
//
// Returns:
//   - nil: Transition is valid
//   - ErrCannotManageHigherRole: Actor lacks authority over current role
//   - ErrCannotPromoteAboveOwnRole: Actor cannot promote to requested role
func (h *OrganizationHandler[U]) ValidateRoleTransition(
	ctx context.Context,
	actorRole proto_models.OrganizationRole,
	currentRole proto_models.OrganizationRole,
	newRole proto_models.OrganizationRole,
) error {
	// Actor must be able to manage the current role
	if !h.canManageRole(actorRole, currentRole) {
		return appLogger.WithErrorFields(errors.ErrCannotManageHigherRole,
			zap.String(logkeys.Operation, "validate_organization_role_transition"),
			zap.Stringer(appLogger.KeyActorRole, actorRole),
			zap.Stringer(appLogger.KeyCurrentRole, currentRole),
			zap.Stringer(appLogger.KeyNewRole, newRole),
			zap.String(logkeys.Reason, "actor_cannot_manage_current_role"),
		)
	}

	// Actor must be able to promote to the new role
	if !h.canGrantRole(actorRole, newRole) {
		return appLogger.WithErrorFields(errors.ErrCannotPromoteAboveOwnRole,
			zap.String(logkeys.Operation, "validate_organization_role_transition"),
			zap.Stringer(appLogger.KeyActorRole, actorRole),
			zap.Stringer(appLogger.KeyCurrentRole, currentRole),
			zap.Stringer(appLogger.KeyNewRole, newRole),
			zap.String(logkeys.Reason, "actor_cannot_promote_to_requested_role"),
		)
	}

	return nil
}

// CanViewOrganization checks if a user can view organization details.
func (h *OrganizationHandler[U]) CanViewOrganization(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
) (bool, error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
		).Debug("CanViewOrganization called")
	}

	if orgID == 0 {
		return false, nil
	}

	user, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return false, appLogger.InvariantError(
			"organization_permission_actor_in_context",
			errors.ErrInternalError,
			zap.String(logkeys.Operation, "view_organization"),
			zap.String(logkeys.Stage, "resolve_actor"),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.String(logkeys.Expected, "authenticated actor in context"),
			zap.String(logkeys.Actual, "actor absent"),
		)
	}

	actorType, actorRole, _, err := h.getActorOrgMembership(ctx, *user, orgID)
	if err != nil {
		return false, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "view_organization"),
			zap.String(logkeys.Stage, "resolve_actor_membership"),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*user).GetID())),
		)
	}
	orgDepth, depthErr := h.getOrgDepthMapped(ctx, orgID)
	if depthErr != nil {
		return false, depthErr
	}

	return h.permissionHandler.HasOrgPermission(
		ctx,
		actorType, actorRole,
		permission.PermissionOrganizationView,
		orgDepth,
	)
}

// CanListAllOrganizations checks if a user can list all organizations in the system.
func (h *OrganizationHandler[U]) CanListAllOrganizations(
	ctx context.Context,
	userType proto_models.UserType,
) (bool, error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Stringer(appLogger.KeyUserType, userType),
		).Debug("CanListAllOrganizations called")
	}

	if h.permissionHandler == nil {
		return false, appLogger.InvariantError(
			"organization_permission_handler_initialized",
			errors.ErrInternalError,
			zap.String(logkeys.Operation, "list_all_organizations"),
			zap.String(logkeys.Stage, "permission_check"),
			zap.String(logkeys.Expected, "permission handler configured"),
			zap.String(logkeys.Actual, "nil permission handler"),
		)
	}

	hasPermission := h.permissionHandler.HasSitePermission(ctx, userType, permission.PermissionAdminListOrganizations)

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Stringer(appLogger.KeyUserType, userType),
		).Debug("list all organizations permission check",
			zap.Stringer(logkeys.Operation, permission.PermissionAdminListOrganizations),
			zap.Bool(appLogger.KeyHasPermission, hasPermission))
	}

	return hasPermission, nil
}

// CanUpdateOrganization checks if a user can update organization details.
func (h *OrganizationHandler[U]) CanUpdateOrganization(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
) (bool, error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
		).Debug("CanUpdateOrganization called")
	}

	if orgID == 0 {
		return false, nil
	}

	user, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return false, appLogger.InvariantError(
			"organization_permission_actor_in_context",
			errors.ErrInternalError,
			zap.String(logkeys.Operation, "update_organization"),
			zap.String(logkeys.Stage, "resolve_actor"),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.String(logkeys.Expected, "authenticated actor in context"),
			zap.String(logkeys.Actual, "actor absent"),
		)
	}

	actorType, actorRole, _, err := h.getActorOrgMembership(ctx, *user, orgID)
	if err != nil {
		return false, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "update_organization"),
			zap.String(logkeys.Stage, "resolve_actor_membership"),
			zap.Uint64(appLogger.KeyOrganizationID, uint64(orgID)),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*user).GetID())),
		)
	}
	orgDepth, depthErr := h.getOrgDepthMapped(ctx, orgID)
	if depthErr != nil {
		return false, depthErr
	}

	return h.permissionHandler.HasOrgPermission(
		ctx,
		actorType, actorRole,
		permission.PermissionOrganizationUpdate,
		orgDepth,
	)
}
