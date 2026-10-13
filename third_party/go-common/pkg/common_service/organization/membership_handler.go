package organization

import (
	"context"
	stdErrors "errors"
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// AddMember adds a new member to an organization with the specified role.
func (h *OrganizationHandler[U]) AddMember(
	ctx context.Context,
	targetUserId common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
	role proto_models.OrganizationRole,
) (*common_service_models.UserOrganizationResponse, error) {
	// Get actor from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)

	if !found {
		return nil, errors.ErrUserNotFound
	}

	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Int64(appLogger.KeyOrganizationID, orgID),
		zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
		zap.Stringer(appLogger.KeyRole, role),
	)

	log = log.With(zap.Uint64(appLogger.KeyActorUserID, uint64((*requestingUser).GetID())))

	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return nil, err
	}
	if err := h.ensurePersonalMembershipAddAllowed(resolvedOrg.Organization, targetUserId, role); err != nil {
		return nil, err
	}

	if err := h.ensureOrganizationTypeCapabilityForOrganization(resolvedOrg.Organization, organizationTypeCapabilityAddMembers); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "add_organization_member"),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*requestingUser).GetID())),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Stringer(appLogger.KeyRole, role),
			zap.String(logkeys.Reason, "organization_type_capability_denied"),
		)
	}

	admissionDiagnostics := membershipAdmissionDiagnostics{
		operation:    "add_organization_member",
		actorID:      (*requestingUser).GetID(),
		orgID:        orgID,
		targetUserID: &targetUserId,
	}
	actorType, actorRole, err := h.resolveMembershipAdmissionActorAndValidateBulkCapability(ctx, *requestingUser, orgID, resolvedOrg, admissionDiagnostics)
	if err != nil {
		return nil, err
	}
	orgDepth := organizationDepth(resolvedOrg.Organization)
	canUpdate, err := h.modifyOtherMemberRolePermissionCheckAtDepth(ctx, actorType, actorRole, targetUserId, orgID, orgDepth, role, nil)
	if err != nil {
		return nil, admissionDiagnostics.errorAtStage(err, "check_role_transition_permission")
	}

	// Also verify actor has AddMembers permission specifically
	canAdd, addErr := h.permissionHandler.HasOrgPermission(ctx, actorType, actorRole, permission.PermissionOrganizationAddMembers, orgDepth)
	if addErr != nil {
		return nil, admissionDiagnostics.internalErrorAtStage(addErr, "check_add_permission")
	}

	if !canUpdate || !canAdd {
		return nil, admissionDiagnostics.denied()
	}

	if targetUserId == (*requestingUser).GetID() {
		parentRole := proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED
		parentDepth := 0
		if orgDepth > 0 {
			parentDepth = orgDepth - 1
		}

		parentOrgID := resolvedOrg.ParentOrganizationID()

		if parentOrgID != 0 {
			_, parentRole, _, err = h.getActorOrgMembership(ctx, *requestingUser, parentOrgID)
			if err != nil {
				return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
					zap.String(logkeys.Operation, "add_organization_member"),
					zap.String(logkeys.Stage, "resolve_parent_role_for_self_join"),
					zap.Uint64(appLogger.KeyActorUserID, uint64((*requestingUser).GetID())),
					zap.Int64(appLogger.KeyOrganizationID, orgID),
					zap.Int64(appLogger.KeyParentOrganizationID, parentOrgID),
				)
			}
		}

		canSelfJoin, err := h.permissionHandler.HasOrgPermission(
			ctx,
			actorType,
			parentRole,
			permission.PermissionOrganizationSelfJoin,
			parentDepth,
		)
		if err != nil {
			return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
				zap.String(logkeys.Operation, "add_organization_member"),
				zap.String(logkeys.Stage, "check_self_join_permission"),
				zap.Uint64(appLogger.KeyActorUserID, uint64((*requestingUser).GetID())),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
			)
		}
		if !canSelfJoin {
			return nil, appLogger.WithErrorFields(errors.ErrOrganizationSelfJoinNotAllowed,
				zap.String(logkeys.Operation, "add_organization_member"),
				zap.Uint64(appLogger.KeyActorUserID, uint64((*requestingUser).GetID())),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
				zap.String(logkeys.Reason, "self_join_threshold_not_met"),
			)
		}
	}

	return h.addMemberPrivilegedWithResolvedOrg(ctx, (*requestingUser).GetID(), targetUserId, resolvedOrg, role)
}

// AddMemberPrivileged adds a member without enforcing org-type capability gating.
func (h *OrganizationHandler[U]) AddMemberPrivileged(
	ctx context.Context,
	targetUserId common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
	role proto_models.OrganizationRole,
) (*common_service_models.UserOrganizationResponse, error) {
	return h.addMemberPrivileged(ctx, h.actorUserIDFromContext(ctx), targetUserId, orgID, role)
}

// addMemberPrivileged resolves organization context and forwards to the privileged member-add path used by trusted callers.
func (h *OrganizationHandler[U]) addMemberPrivileged(
	ctx context.Context,
	actorID common_service_models.UserIdType,
	targetUserId common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
	role proto_models.OrganizationRole,
) (*common_service_models.UserOrganizationResponse, error) {
	if targetUserId == 0 {
		return nil, errors.ErrInvalidUserID
	}
	if orgID == 0 {
		return nil, errors.ErrInvalidOrganizationID
	}

	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return nil, err
	}

	return h.addMemberPrivilegedWithResolvedOrg(ctx, actorID, targetUserId, resolvedOrg, role)
}

// addMemberPrivilegedWithResolvedOrg performs privileged membership creation using a pre-resolved organization and enforces ancestor membership prerequisites.
func (h *OrganizationHandler[U]) addMemberPrivilegedWithResolvedOrg(
	ctx context.Context,
	actorID common_service_models.UserIdType,
	targetUserId common_service_models.UserIdType,
	resolvedOrg *resolvedOrganization,
	role proto_models.OrganizationRole,
) (*common_service_models.UserOrganizationResponse, error) {
	if targetUserId == 0 {
		return nil, errors.ErrInvalidUserID
	}
	if resolvedOrg == nil || resolvedOrg.Organization == nil {
		return nil, errors.ErrOrganizationNotFound
	}
	if err := h.ensurePersonalMembershipAddAllowed(resolvedOrg.Organization, targetUserId, role); err != nil {
		return nil, err
	}

	if err := h.ensureAncestorMembershipForLineage(ctx, targetUserId, resolvedOrg.Organization.Lineage, h.runtimeConfig()); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "add_organization_member"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, resolvedOrg.Organization.ID),
			zap.Stringer(appLogger.KeyRole, role),
			zap.String(logkeys.Reason, "missing_ancestor_membership"),
		)
	}

	return h.addMemberDirectWithResolvedOrg(ctx, actorID, targetUserId, resolvedOrg, role)
}

// addMemberDirect handles the internal logic for adding members when the actor ID and target organization ID are already known.
func (h *OrganizationHandler[U]) addMemberDirect(
	ctx context.Context,
	actorID common_service_models.UserIdType,
	targetUserId common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
	role proto_models.OrganizationRole,
) (*common_service_models.UserOrganizationResponse, error) {
	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return nil, err
	}

	return h.addMemberDirectWithResolvedOrg(ctx, actorID, targetUserId, resolvedOrg, role)
}

// addMemberDirectWithResolvedOrg inserts a direct membership row and returns enriched role details using a pre-resolved organization.
func (h *OrganizationHandler[U]) addMemberDirectWithResolvedOrg(
	ctx context.Context,
	actorID common_service_models.UserIdType,
	targetUserId common_service_models.UserIdType,
	resolvedOrg *resolvedOrganization,
	role proto_models.OrganizationRole,
) (*common_service_models.UserOrganizationResponse, error) {
	if resolvedOrg == nil || resolvedOrg.Organization == nil {
		return nil, errors.ErrOrganizationNotFound
	}
	if err := h.ensurePersonalMembershipAddAllowed(resolvedOrg.Organization, targetUserId, role); err != nil {
		return nil, err
	}

	orgID := resolvedOrg.Organization.ID
	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
		zap.Int64(appLogger.KeyOrganizationID, orgID),
		zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
		zap.Stringer(appLogger.KeyRole, role),
	)
	log.Debug("AddMemberDirect called")

	// Check if user is already a member
	userOrg, err := h.persistenceHandler.GetUserOrganization(ctx, uint64(targetUserId), orgID)
	if err != nil && !stdErrors.Is(err, persistence.ErrUserOrganizationNotFound) {
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "add_organization_member"),
			zap.String(logkeys.Stage, "check_existing_membership"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Stringer(appLogger.KeyRole, role),
		)
	}

	if userOrg != nil {
		return nil, appLogger.WithErrorFields(errors.ErrUserAlreadyMember,
			zap.String(logkeys.Operation, "add_organization_member"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Stringer(appLogger.KeyRole, role),
			zap.String(logkeys.Reason, "already_member"),
		)
	}

	// Add user to organization
	err = h.persistenceHandler.AddUserToOrganization(ctx, targetUserId, orgID, role)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationAncestorMembershipRequired) {
			return nil, errors.ErrOrganizationAncestorMembershipRequired
		}
		if stdErrors.Is(err, persistence.ErrUserAlreadyInOrganization) {
			return nil, errors.ErrUserAlreadyMember
		}
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return nil, errors.ErrOrganizationNotFound
		}
		if stdErrors.Is(err, persistence.ErrUserNotFound) {
			return nil, errors.ErrUserNotFound
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "add_organization_member"),
			zap.String(logkeys.Stage, "persist_membership"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Stringer(appLogger.KeyRole, role),
		)
	}

	// Invalidate role cache for this user and descendants
	h.InvalidateRoleCacheForDescendants(ctx, targetUserId, orgID)

	// Get updated membership information
	targetMembership, err := h.persistenceHandler.GetUserOrganization(ctx, uint64(targetUserId), orgID)
	if err != nil {
		return nil, appLogger.InvariantError(
			"organization_membership_readable_after_create",
			appLogger.WithErrorCause(errors.ErrInternalError, err),
			zap.String(logkeys.Operation, "add_organization_member"),
			zap.String(logkeys.Stage, "reload_membership"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.String(logkeys.Expected, "created membership can be reloaded"),
			zap.String(logkeys.Actual, "membership reload failed"),
		)
	}

	// Convert to service model and create response
	serviceUserOrg := h.converter.PersistenceUserOrganizationToService(targetMembership)
	if serviceUserOrg.EffectiveRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		depth := uint32(organizationDepth(resolvedOrg.Organization))
		serviceUserOrg.EffectiveRoleDepth = &depth
	}
	h.applyMembershipRoleProviderOverride(ctx, serviceUserOrg, targetUserId, orgID)
	response := &common_service_models.UserOrganizationResponse{
		UserOrganization: serviceUserOrg,
	}

	if err := h.invokeOnOrganizationMemberAdded(ctx, serviceUserOrg); err != nil {
		return nil, appLogger.WithErrorFields(
			fmt.Errorf("%w: %w", errors.ErrOrganizationCallbackFailed, err),
			zap.String(logkeys.Operation, "add_organization_member"),
			zap.String(logkeys.Stage, "member_added_callback"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
		)
	}

	log.Info("user added to organization successfully")
	return response, nil
}

func (h *OrganizationHandler[U]) applyMembershipRoleProviderOverride(
	ctx context.Context,
	membership *common_service_models.UserOrganization,
	userID common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
) {
	if h.orgRoleProvider == nil {
		return
	}
	roleInfo, _, err := h.orgRoleProvider.GetUserRole(ctx, userID, orgID)
	if err != nil {
		return
	}
	membership.ExplicitRole = roleInfo.ExplicitRole
	membership.EffectiveRole = roleInfo.EffectiveRole
	membership.InheritedFromOrganizationID = roleInfo.InheritedFromOrganizationID
	membership.EffectiveRoleDepth = roleInfo.EffectiveRoleDepth
}

// RemoveMember removes a member from an organization.
func (h *OrganizationHandler[U]) RemoveMember(
	ctx context.Context,
	targetUserId common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
) error {
	// Get actor from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return errors.ErrUserNotFound
	}
	actor := *requestingUser

	return h.removeMemberDirect(ctx, actor, targetUserId, orgID)
}

// removeMemberDirect handles the internal member-removal logic once the acting user model is already available.
func (h *OrganizationHandler[U]) removeMemberDirect(
	ctx context.Context,
	actor common_service_models.UserLike,
	targetUserId common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
) error {
	if actor == nil {
		return errors.ErrUserNotFound
	}
	actorID := actor.GetID()

	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Int64(appLogger.KeyOrganizationID, orgID),
		zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
	)

	log = log.With(zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)))
	log.Debug("RemoveMember called")

	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return err
	}
	if err := h.ensurePersonalMemberRemovalAllowed(resolvedOrg.Organization, targetUserId); err != nil {
		return err
	}

	if err := h.ensureOrganizationTypeCapabilityForOrganization(resolvedOrg.Organization, organizationTypeCapabilityRemoveMembers); err != nil {
		return appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "remove_organization_member"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.String(logkeys.Reason, "organization_type_capability_denied"),
		)
	}

	// Check access permissions - delegate to permission handler
	if h.permissionHandler == nil {
		return appLogger.InvariantError(
			"organization_permission_handler_initialized",
			errors.ErrInternalError,
			zap.String(logkeys.Operation, "remove_organization_member"),
			zap.String(logkeys.Stage, "permission_check"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.String(logkeys.Expected, "permission handler configured"),
			zap.String(logkeys.Actual, "nil permission handler"),
		)
	}
	actorType, actorRole, isMember, err := h.getActorOrgMembership(ctx, actor, orgID)
	if err != nil {
		return appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "remove_organization_member"),
			zap.String(logkeys.Stage, "resolve_actor_membership"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
		)
	}

	// Site admins may remove members without holding an org membership, mirroring the
	// shortcut honored by AddMember and UpdateMemberRole.
	isSiteAdmin := h.permissionHandler.HasSitePermission(ctx, actorType, permission.PermissionAdminManageOrganizationMembers)

	canRemove, err := h.permissionHandler.HasOrgPermission(
		ctx,
		actorType,
		actorRole,
		permission.PermissionOrganizationRemoveMembers,
		organizationDepth(resolvedOrg.Organization),
	)
	if err != nil {
		return appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "remove_organization_member"),
			zap.String(logkeys.Stage, "permission_check"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
		)
	}
	if !canRemove {
		return appLogger.WithErrorFields(errors.ErrInsufficientPermissions,
			zap.String(logkeys.Operation, "remove_organization_member"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.String(logkeys.Reason, "insufficient_permission"),
		)
	}

	// Get target user role
	targetRole, err := h.getUserRoleInOrganization(ctx, targetUserId, orgID)
	if err != nil {
		if stdErrors.Is(err, errors.ErrUserNotMember) {
			return errors.ErrUserNotMember
		}
		return appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "remove_organization_member"),
			zap.String(logkeys.Stage, "resolve_target_role"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
		)
	}

	var memberForCallback *common_service_models.UserOrganization
	callbacks := h.organizationCallbacks()
	if callbacks != nil && callbacks.OnOrganizationMemberRemoved != nil {
		membership, membershipErr := h.persistenceHandler.GetUserOrganization(ctx, uint64(targetUserId), orgID)
		if membershipErr == nil {
			memberForCallback = h.converter.PersistenceUserOrganizationToService(membership)
		} else if stdErrors.Is(membershipErr, persistence.ErrUserOrganizationNotFound) {
			memberForCallback = &common_service_models.UserOrganization{
				OrganizationID: orgID,
				UserID:         targetUserId,
				ExplicitRole:   targetRole,
				EffectiveRole:  targetRole,
			}
		} else {
			return appLogger.WithErrorCause(errors.ErrInternalError, membershipErr,
				zap.String(logkeys.Operation, "remove_organization_member"),
				zap.String(logkeys.Stage, "load_membership_for_callback"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
			)
		}
	}

	// Check if actor can remove this member (unless removing themselves or acting
	// with site-admin authority)
	if actorID != targetUserId && !isSiteAdmin {
		if !isMember {
			return appLogger.WithErrorFields(errors.ErrInsufficientOrganizationPermissions,
				zap.String(logkeys.Operation, "remove_organization_member"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
				zap.String(logkeys.Reason, "actor_not_member"),
			)
		}

		// Check if actor can manage the target user's role
		if !common_service_models.CanManageRole(actorRole, targetRole) {
			return appLogger.WithErrorFields(errors.ErrCannotManageHigherRole,
				zap.String(logkeys.Operation, "remove_organization_member"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
				zap.String(logkeys.Reason, "actor_cannot_manage_target_role"),
			)
		}
	}

	// Remove membership. The last explicit owner cannot be removed (including self-removal);
	// ownership must be transferred first so the organization never becomes unmanageable. The
	// guard runs atomically with the delete under a per-organization lock so concurrent removals
	// cannot both pass and leave the organization ownerless, and it keys off the target's explicit
	// owner role (not an inherited one) so purely-inherited owners are not falsely blocked.
	err = h.persistenceHandler.RemoveUserFromOrganizationAndDescendantsGuardingLastOwner(
		ctx, uint64(targetUserId), orgID, proto_models.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_0)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationLastOwner) {
			return appLogger.WithErrorFields(errors.ErrCannotRemoveLastOwner,
				zap.String(logkeys.Operation, "remove_organization_member"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
				zap.String(logkeys.Reason, "last_owner"),
			)
		}
		return appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "remove_organization_member"),
			zap.String(logkeys.Stage, "persist_removal"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
		)
	}

	h.InvalidateRoleCacheForDescendants(ctx, targetUserId, orgID)

	if memberForCallback != nil {
		if err := h.invokeOnOrganizationMemberRemoved(ctx, memberForCallback); err != nil {
			return appLogger.WithErrorFields(
				fmt.Errorf("%w: %w", errors.ErrOrganizationCallbackFailed, err),
				zap.String(logkeys.Operation, "remove_organization_member"),
				zap.String(logkeys.Stage, "member_removed_callback"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
			)
		}
	}

	log.Info("member removed from organization successfully")
	return nil
}

// UpdateMemberRole updates a member's role within an organization.
// Only organization owners/admins can update member roles, and they cannot assign roles higher than their own.
func (h *OrganizationHandler[U]) UpdateMemberRole(
	ctx context.Context,
	targetUserId common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
	newRole proto_models.OrganizationRole,
) (*common_service_models.UserOrganizationResponse, error) {
	// Get actor from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, errors.ErrUserNotFound
	}

	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Int64(appLogger.KeyOrganizationID, orgID),
		zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
		zap.Stringer(appLogger.KeyNewRole, newRole),
	)

	log = log.With(zap.Uint64(appLogger.KeyActorUserID, uint64((*requestingUser).GetID())))

	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return nil, err
	}
	if err := h.ensurePersonalMemberRoleUpdateAllowed(resolvedOrg.Organization, targetUserId, newRole); err != nil {
		return nil, err
	}

	if err := h.ensureOrganizationTypeCapabilityForOrganization(resolvedOrg.Organization, organizationTypeCapabilityUpdateMemberRoles); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "update_organization_member_role"),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*requestingUser).GetID())),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Stringer(appLogger.KeyNewRole, newRole),
			zap.String(logkeys.Reason, "organization_type_capability_denied"),
		)
	}

	// Validate inputs
	if orgID == 0 {
		return nil, errors.ErrInvalidOrganizationID
	}
	if targetUserId == 0 {
		return nil, errors.ErrInvalidUserID
	}
	if newRole == proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		return nil, errors.ErrInvalidOrganizationRole
	}

	// Get actor membership
	actorType, actorRole, _, err := h.getActorOrgMembership(ctx, *requestingUser, orgID)
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "update_organization_member_role"),
			zap.String(logkeys.Stage, "resolve_actor_membership"),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*requestingUser).GetID())),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
		)
	}

	// Get target user's current role for permission check
	targetRole, err := h.getUserRoleInOrganization(ctx, targetUserId, orgID)
	if err != nil {
		if stdErrors.Is(err, errors.ErrUserNotMember) {
			return nil, errors.ErrUserNotMember
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "update_organization_member_role"),
			zap.String(logkeys.Stage, "resolve_target_role"),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*requestingUser).GetID())),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
		)
	}

	// Check role modification permission
	canUpdate, err := h.modifyOtherMemberRolePermissionCheckAtDepth(ctx, actorType, actorRole, targetUserId, orgID, organizationDepth(resolvedOrg.Organization), newRole, &targetRole)
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "update_organization_member_role"),
			zap.String(logkeys.Stage, "permission_check"),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*requestingUser).GetID())),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Stringer(appLogger.KeyNewRole, newRole),
		)
	}
	if !canUpdate {
		return nil, appLogger.WithErrorFields(errors.ErrInsufficientPermissions,
			zap.String(logkeys.Operation, "update_organization_member_role"),
			zap.Uint64(appLogger.KeyActorUserID, uint64((*requestingUser).GetID())),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Stringer(appLogger.KeyNewRole, newRole),
			zap.String(logkeys.Reason, "insufficient_permission"),
		)
	}

	// Demoting the last explicit owner below owner would leave the organization unmanageable;
	// ownership must be transferred first. The guard is enforced atomically with the update in the
	// persistence layer (see updateMemberRoleDirect) so concurrent demotions cannot both pass.
	return h.updateMemberRolePrivilegedWithResolvedOrg(ctx, (*requestingUser).GetID(), targetUserId, resolvedOrg, newRole, true)
}

// UpdateMemberRolePrivileged updates a member role without enforcing org-type capability gating.
func (h *OrganizationHandler[U]) UpdateMemberRolePrivileged(
	ctx context.Context,
	targetUserId common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
	newRole proto_models.OrganizationRole,
) (*common_service_models.UserOrganizationResponse, error) {
	return h.updateMemberRolePrivileged(ctx, h.actorUserIDFromContext(ctx), targetUserId, orgID, newRole)
}

// updateMemberRolePrivileged resolves organization context and delegates to the privileged role-update flow used by trusted callers.
func (h *OrganizationHandler[U]) updateMemberRolePrivileged(
	ctx context.Context,
	actorID common_service_models.UserIdType,
	targetUserId common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
	newRole proto_models.OrganizationRole,
) (*common_service_models.UserOrganizationResponse, error) {
	if orgID == 0 {
		return nil, errors.ErrInvalidOrganizationID
	}
	if targetUserId == 0 {
		return nil, errors.ErrInvalidUserID
	}
	if newRole == proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		return nil, errors.ErrInvalidOrganizationRole
	}

	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return nil, err
	}

	// Trusted privileged callers bypass the last-owner demotion guard.
	return h.updateMemberRolePrivilegedWithResolvedOrg(ctx, actorID, targetUserId, resolvedOrg, newRole, false)
}

// updateMemberRolePrivilegedWithResolvedOrg updates a member role with a pre-resolved organization and shared privileged semantics.
func (h *OrganizationHandler[U]) updateMemberRolePrivilegedWithResolvedOrg(
	ctx context.Context,
	actorID common_service_models.UserIdType,
	targetUserId common_service_models.UserIdType,
	resolvedOrg *resolvedOrganization,
	newRole proto_models.OrganizationRole,
	guardLastOwner bool,
) (*common_service_models.UserOrganizationResponse, error) {
	if resolvedOrg == nil || resolvedOrg.Organization == nil {
		return nil, errors.ErrOrganizationNotFound
	}
	if targetUserId == 0 {
		return nil, errors.ErrInvalidUserID
	}
	if newRole == proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		return nil, errors.ErrInvalidOrganizationRole
	}
	if err := h.ensurePersonalMemberRoleUpdateAllowed(resolvedOrg.Organization, targetUserId, newRole); err != nil {
		return nil, err
	}

	_, err := h.getUserRoleInOrganization(ctx, targetUserId, resolvedOrg.Organization.ID)
	if err != nil {
		if stdErrors.Is(err, errors.ErrUserNotMember) {
			return nil, errors.ErrUserNotMember
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "update_organization_member_role"),
			zap.String(logkeys.Stage, "resolve_target_role"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Int64(appLogger.KeyOrganizationID, resolvedOrg.Organization.ID),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Stringer(appLogger.KeyNewRole, newRole),
		)
	}

	return h.updateMemberRoleDirect(ctx, actorID, targetUserId, resolvedOrg.Organization.ID, newRole, guardLastOwner)
}

// modifyOtherMemberRolePermissionCheckAtDepth evaluates role-modification permissions with a known organization depth, including site-admin shortcut logic.
func (h *OrganizationHandler[U]) modifyOtherMemberRolePermissionCheckAtDepth(
	ctx context.Context,
	actorUserType proto_models.UserType,
	actorMemberRole proto_models.OrganizationRole,
	targetUserId common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
	orgDepth int,
	newRole proto_models.OrganizationRole,
	currentTargetRole *proto_models.OrganizationRole,
) (bool, error) {
	fields := []zap.Field{
		zap.Int64(appLogger.KeyOrganizationID, orgID),
		zap.Uint64(appLogger.KeyTargetUserID, uint64(targetUserId)),
		zap.Stringer(appLogger.KeyActorUserType, actorUserType),
		zap.Stringer(appLogger.KeyActorMemberRole, actorMemberRole),
		zap.Stringer(appLogger.KeyNewRole, newRole),
	}
	if currentTargetRole != nil {
		fields = append(fields, zap.Stringer(appLogger.KeyCurrentTargetRole, currentTargetRole))
	} else {
		fields = append(fields, zap.String(appLogger.KeyCurrentTargetRole, "unknown"))
	}

	if h.permissionHandler == nil {
		return false, appLogger.InvariantError(
			"organization_permission_handler_initialized",
			errors.ErrInternalError,
			append(fields,
				zap.String(logkeys.Operation, "modify_organization_member_role_permission"),
				zap.String(logkeys.Stage, "permission_check"),
				zap.String(logkeys.Expected, "permission handler configured"),
				zap.String(logkeys.Actual, "nil permission handler"),
			)...,
		)
	}
	// This helper only evaluates role and user-type permissions for modifying another
	// member's role. Callers must enforce any required org-type bulk/admin capability
	// gates before invoking it. In particular, InviteUserToOrganization and
	// AddUserToOrganizationByIdentifier need top-level bulk_member_management checks
	// if they rely on this site-admin shortcut.
	if h.permissionHandler.HasSitePermission(ctx, actorUserType, permission.PermissionAdminManageOrganizationMembers) {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, organizationLoggerName).With(fields...).
				Debug("site-admin permission shortcut granted; callers must enforce org-type bulk-management checks before invoking this helper")
		}
		return true, nil
	}

	canUpdate, err := h.permissionHandler.HasOrgPermission(ctx, actorUserType, actorMemberRole, permission.PermissionOrganizationUpdateMemberRoles, orgDepth)
	if err != nil {
		return false, appLogger.WithErrorCause(errors.ErrInternalError, err,
			append(fields,
				zap.String(logkeys.Operation, "modify_organization_member_role_permission"),
				zap.String(logkeys.Stage, "permission_check"),
			)...,
		)
	}
	canUpdate = canUpdate && common_service_models.CanPromoteToRole(actorMemberRole, newRole)
	if currentTargetRole != nil {
		canUpdate = canUpdate && common_service_models.CanPromoteToRole(actorMemberRole, *currentTargetRole)
	}
	return canUpdate, nil
}

// updateMemberRoleDirect handles the internal role-update logic once actor, target, and organization IDs are already known.
func (h *OrganizationHandler[U]) updateMemberRoleDirect(
	ctx context.Context,
	actorID common_service_models.UserIdType,
	targetUserId common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
	newRole proto_models.OrganizationRole,
	guardLastOwner bool,
) (*common_service_models.UserOrganizationResponse, error) {
	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
		zap.Int64(appLogger.KeyOrganizationID, orgID),
		zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
		zap.Stringer(appLogger.KeyNewRole, newRole),
	)
	log.Debug("updateMemberRoleDirect called")

	// Update role. When the last-owner guard applies, demoting the sole explicit owner below owner
	// is refused atomically with the update (under a per-organization lock) so concurrent demotions
	// cannot both pass; the guard keys off the target's explicit owner role, so purely-inherited
	// owners are not falsely blocked.
	var err error
	if guardLastOwner {
		err = h.persistenceHandler.UpdateUserOrganizationRoleGuardingLastOwner(
			ctx, uint64(targetUserId), orgID, newRole, proto_models.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_0)
		if err != nil && stdErrors.Is(err, persistence.ErrOrganizationLastOwner) {
			return nil, appLogger.WithErrorFields(errors.ErrCannotDemoteLastOwner,
				zap.String(logkeys.Operation, "update_organization_member_role"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
				zap.Stringer(appLogger.KeyNewRole, newRole),
				zap.String(logkeys.Reason, "last_owner"),
			)
		}
	} else {
		err = h.persistenceHandler.UpdateUserOrganizationRole(ctx, uint64(targetUserId), orgID, newRole)
	}
	if err != nil {
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "update_organization_member_role"),
			zap.String(logkeys.Stage, "persist_role"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Stringer(appLogger.KeyNewRole, newRole),
		)
	}

	h.InvalidateRoleCacheForDescendants(ctx, targetUserId, orgID)

	// Get updated membership to return in response
	updatedMembership, err := h.persistenceHandler.GetUserOrganization(ctx, uint64(targetUserId), orgID)
	if err != nil {
		return nil, appLogger.InvariantError(
			"organization_membership_readable_after_role_update",
			appLogger.WithErrorCause(errors.ErrInternalError, err),
			zap.String(logkeys.Operation, "update_organization_member_role"),
			zap.String(logkeys.Stage, "reload_membership"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Uint64(appLogger.KeyUserID, uint64(targetUserId)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Stringer(appLogger.KeyNewRole, newRole),
			zap.String(logkeys.Expected, "updated membership can be reloaded"),
			zap.String(logkeys.Actual, "membership reload failed"),
		)
	}

	// Convert to service model and create response
	serviceMembership := h.converter.PersistenceUserOrganizationToService(updatedMembership)
	if serviceMembership.EffectiveRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		if depth, depthErr := h.getOrgDepth(ctx, orgID); depthErr == nil {
			convertedDepth := uint32(depth)
			serviceMembership.EffectiveRoleDepth = &convertedDepth
		}
	}
	h.applyMembershipRoleProviderOverride(ctx, serviceMembership, targetUserId, orgID)
	response := &common_service_models.UserOrganizationResponse{
		UserOrganization: serviceMembership,
	}

	log.Info("member role updated successfully")
	return response, nil
}

// GetOrganizationMembers retrieves the members of an organization with pagination.
// Returns enriched member details including user profile information.
func (h *OrganizationHandler[U]) GetOrganizationMembers(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
	limit, offset int,
) ([]*common_service_models.OrganizationMemberDetails, int64, error) {
	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Int64(appLogger.KeyOrganizationID, orgID),
		zap.Int(logkeys.Limit, limit),
		zap.Int(appLogger.KeyOffset, offset),
	)
	log.Debug("GetOrganizationMembers called")

	// Validate input
	if orgID == 0 {
		return nil, 0, errors.ErrInvalidOrganizationID
	}
	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return nil, 0, err
	}
	if err := h.ensureOrganizationTypeCapabilityForOrganization(resolvedOrg.Organization, organizationTypeCapabilityListMembers); err != nil {
		return nil, 0, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "list_organization_members"),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.String(logkeys.Reason, "organization_type_capability_denied"),
		)
	}

	actorID, err := h.authorizeOrganizationMemberRead(ctx, orgID, organizationDepth(resolvedOrg.Organization))
	if err != nil {
		return nil, 0, err
	}
	log = log.With(zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)))

	if err := requireListLimit("list_organization_members", limit); err != nil {
		return nil, 0, err
	}

	if offset < 0 {
		offset = 0
	}

	// Get members from persistence layer (User is preloaded via GORM Preload)
	persistenceMembers, total, err := h.persistenceHandler.GetOrganizationUsers(ctx, orgID, proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, offset, limit)
	if err != nil {
		return nil, 0, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "list_organization_members"),
			zap.String(logkeys.Stage, "load_members"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Int(logkeys.Limit, limit),
			zap.Int(appLogger.KeyOffset, offset),
		)
	}

	// Build service models (no proto conversion, no permission filtering)
	serviceMembers := common_util.ApplyToEachFiltered(
		persistenceMembers,
		func(persistenceMember *common_persistence_models.UserOrganization) *common_service_models.OrganizationMemberDetails {
			memberDetails := h.converter.PersistenceUserOrganizationToServiceMemberDetails(persistenceMember)
			if memberDetails == nil {
				return nil
			}
			return memberDetails
		},
		func(persistenceMember *common_persistence_models.UserOrganization) bool {
			return persistenceMember != nil
		},
		nil,
	)

	if len(serviceMembers) > 0 {
		depth := uint32(organizationDepth(resolvedOrg.Organization))
		for _, memberDetails := range serviceMembers {
			if memberDetails == nil || memberDetails.EffectiveRole == proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
				continue
			}
			memberDetails.EffectiveRoleDepth = &depth
		}
	}

	// Avoid N+1 lookups by resolving effective roles for the full page in one query.
	if h.orgRoleProvider != nil && h.runtimeConfig().OrgRoleInheritanceEnabled() && len(serviceMembers) > 0 {
		userIDs := make([]uint64, 0, len(serviceMembers))
		for _, memberDetails := range serviceMembers {
			if memberDetails == nil {
				continue
			}
			userIDs = append(userIDs, uint64(memberDetails.UserID))
		}
		effectiveRoleByUserID, roleErr := h.persistenceHandler.GetUsersEffectiveOrganizationRolesByLineage(ctx, userIDs, orgID)
		if roleErr != nil {
			return nil, 0, appLogger.WithErrorCause(errors.ErrInternalError, roleErr,
				zap.String(logkeys.Operation, "list_organization_members"),
				zap.String(logkeys.Stage, "resolve_effective_roles"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
				zap.Int(appLogger.KeyCount, len(userIDs)),
			)
		}
		for _, memberDetails := range serviceMembers {
			if memberDetails == nil {
				continue
			}
			roleResolution, found := effectiveRoleByUserID[uint64(memberDetails.UserID)]
			if !found {
				continue
			}
			memberDetails.ExplicitRole = roleResolution.ExplicitRole
			memberDetails.EffectiveRole = roleResolution.EffectiveRole
			if roleResolution.InheritedFromOrganizationID != nil {
				converted := common_service_models.OrganizationIdType(*roleResolution.InheritedFromOrganizationID)
				memberDetails.InheritedFromOrganizationID = &converted
			} else {
				memberDetails.InheritedFromOrganizationID = nil
			}
			if roleResolution.EffectiveRoleDepth != nil && *roleResolution.EffectiveRoleDepth >= 0 {
				convertedDepth := uint32(*roleResolution.EffectiveRoleDepth)
				memberDetails.EffectiveRoleDepth = &convertedDepth
			} else {
				memberDetails.EffectiveRoleDepth = nil
			}
		}
	}

	if ce := log.Check(zap.DebugLevel, "organization members retrieved successfully"); ce != nil {
		ce.Write(
			zap.Int64(appLogger.KeyTotal, total),
			zap.Int(appLogger.KeyReturned, len(serviceMembers)),
		)
	}
	return serviceMembers, total, nil
}

// GetUserOrganizations retrieves all organizations a user is a member of.
func (h *OrganizationHandler[U]) GetUserOrganizations(
	ctx context.Context,
	userID common_service_models.UserIdType,
) ([]*common_service_models.UserOrganizationResponse, error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		).Debug("GetUserOrganizations called")
	}

	if userID == 0 {
		return nil, errors.ErrInvalidUserID
	}

	// Get user organizations from persistence layer.
	persistenceMemberships, _, err := h.persistenceHandler.GetUserOrganizations(ctx, uint64(userID), nil)
	if err != nil {
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "get_user_organizations"),
			zap.String(logkeys.Stage, "load_memberships"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		)
	}

	// Convert to service models and create responses
	minimumType := h.runtimeConfig().OrgMinTypeWorkspaceNavigation()
	responses := make([]*common_service_models.UserOrganizationResponse, 0, len(persistenceMemberships))
	for _, persistenceMembership := range persistenceMemberships {
		if persistenceMembership == nil || persistenceMembership.UserOrganization == nil {
			continue
		}
		if (persistenceMembership.UserOrganization.Organization.OrganizationType) < minimumType {
			continue
		}
		serviceMembership := h.converter.PersistenceUserOrganizationToService(persistenceMembership.UserOrganization)
		serviceMembership.ExplicitRole = persistenceMembership.RoleInfo.ExplicitRole
		serviceMembership.EffectiveRole = persistenceMembership.RoleInfo.EffectiveRole
		serviceMembership.InheritedFromOrganizationID = persistenceMembership.RoleInfo.InheritedFromOrganizationID
		if persistenceMembership.RoleInfo.EffectiveRoleDepth != nil {
			depth := uint32(*persistenceMembership.RoleInfo.EffectiveRoleDepth)
			serviceMembership.EffectiveRoleDepth = &depth
		}
		responses = append(responses, &common_service_models.UserOrganizationResponse{
			UserOrganization: serviceMembership,
		})
	}

	return responses, nil
}

// GetUserOrganizationMembership retrieves the complete membership details of a user within a specific organization.
// Note: This method bypasses the role cache to fetch fresh data, but updates the cache with the result.
func (h *OrganizationHandler[U]) GetUserOrganizationMembership(
	ctx context.Context,
	userID common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
) (*common_service_models.OrganizationMemberDetails, error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
		).Debug("GetUserOrganizationMembership called")
	}

	if userID == 0 {
		return nil, errors.ErrInvalidUserID
	}
	if orgID == 0 {
		return nil, errors.ErrInvalidOrganizationID
	}
	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return nil, err
	}
	if err := h.ensureOrganizationTypeCapabilityForOrganization(resolvedOrg.Organization, organizationTypeCapabilityListMembers); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "get_organization_membership"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.String(logkeys.Reason, "organization_type_capability_denied"),
		)
	}

	// Attribute filtering later in the API response is not an object-access
	// control: even the most restrictive filter preserves membership and role
	// metadata, so authorize before querying the target membership.
	if _, err := h.authorizeOrganizationMemberRead(ctx, orgID, organizationDepth(resolvedOrg.Organization)); err != nil {
		return nil, err
	}

	return h.getUserOrganizationMembershipPrivilegedWithResolvedOrg(ctx, userID, resolvedOrg)
}

// GetUserOrganizationMembershipPrivileged retrieves membership details without
func (h *OrganizationHandler[U]) GetUserOrganizationMembershipPrivileged(
	ctx context.Context,
	userID common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
) (*common_service_models.OrganizationMemberDetails, error) {
	if userID == 0 {
		return nil, errors.ErrInvalidUserID
	}
	if orgID == 0 {
		return nil, errors.ErrInvalidOrganizationID
	}

	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return nil, err
	}

	return h.getUserOrganizationMembershipPrivilegedWithResolvedOrg(ctx, userID, resolvedOrg)
}

// getUserOrganizationMembershipPrivilegedWithResolvedOrg loads and enriches membership details using a pre-resolved organization without org-type gating.
func (h *OrganizationHandler[U]) getUserOrganizationMembershipPrivilegedWithResolvedOrg(
	ctx context.Context,
	userID common_service_models.UserIdType,
	resolvedOrg *resolvedOrganization,
) (*common_service_models.OrganizationMemberDetails, error) {
	if userID == 0 {
		return nil, errors.ErrInvalidUserID
	}
	if resolvedOrg == nil || resolvedOrg.Organization == nil {
		return nil, errors.ErrOrganizationNotFound
	}

	orgID := resolvedOrg.Organization.ID

	// Fetch membership data from persistence
	persistenceMembership, err := h.persistenceHandler.GetUserOrganization(ctx, uint64(userID), orgID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrUserOrganizationNotFound) {
			return nil, appLogger.WithErrorFields(errors.ErrUserNotMember,
				zap.String(logkeys.Operation, "get_user_organization_membership"),
				zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
				zap.String(logkeys.Reason, "not_member"),
			)
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "get_user_organization_membership"),
			zap.String(logkeys.Stage, "load_membership"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
		)
	}

	// Convert preloaded persistence user to service user for profile data
	// User data is preloaded by GetUserOrganization via GORM Preload
	serviceUser := h.converter.ConvertPersistenceToUser(&persistenceMembership.User)

	// Build enriched member details with membership and user profile data
	memberDetails := &common_service_models.OrganizationMemberDetails{
		OrganizationID: orgID,
		UserID:         userID,
		ExplicitRole:   persistenceMembership.Role,
		EffectiveRole:  persistenceMembership.Role,
		CreatedAt:      persistenceMembership.JoinedAt,
		UpdatedAt:      persistenceMembership.UpdatedAt,
		Username:       serviceUser.Username,
		Email:          serviceUser.Email,
		FirstName:      serviceUser.FirstName,
		LastName:       serviceUser.LastName,
		AvatarUrl:      serviceUser.AvatarUrl,
		UserType:       serviceUser.UserType,
		Banned:         serviceUser.Banned,
		IsActive:       !serviceUser.Banned,
		LastActivity:   nil, // LastActivity is not currently tracked in User model
	}
	if memberDetails.EffectiveRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		convertedDepth := uint32(organizationDepth(resolvedOrg.Organization))
		memberDetails.EffectiveRoleDepth = &convertedDepth
	}
	if h.orgRoleProvider != nil {
		roleInfo, _, roleErr := h.orgRoleProvider.GetUserRole(ctx, userID, orgID)
		if roleErr == nil {
			memberDetails.ExplicitRole = roleInfo.ExplicitRole
			memberDetails.EffectiveRole = roleInfo.EffectiveRole
			memberDetails.InheritedFromOrganizationID = roleInfo.InheritedFromOrganizationID
			memberDetails.EffectiveRoleDepth = roleInfo.EffectiveRoleDepth
		}
	}

	if appLogger.DebugEnabled(h.logger) {
		log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
		)
		log.Debug("user organization membership retrieved successfully with enriched profile data",
			zap.Stringer(appLogger.KeyRole, persistenceMembership.Role))
	}
	return memberDetails, nil
}
