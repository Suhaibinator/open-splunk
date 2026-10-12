package organization

import (
	"context"
	stdErrors "errors"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// AddUserResult contains the result of a direct add attempt by identifier.
type AddUserResult struct {
	UserAdded        bool
	AutoAddPending   bool
	OrganizationUser *common_service_models.UserOrganizationResponse
	AutoAddEntry     *common_service_models.OrganizationInvitation
}

// InviteUserToOrganization invites a user to join an organization.
func (h *OrganizationHandler[U]) InviteUserToOrganization(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
	identifierType proto_models.UserIdentifierType,
	identifier string,
	role proto_models.OrganizationRole,
) (uint64, error) {
	if err := h.requireInvitationNotificationTransaction(ctx); err != nil {
		return 0, err
	}
	// Get actor from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return 0, errors.ErrUserNotFound
	}
	actor := *requestingUser
	actorID := actor.GetID()
	runtimeConfig := h.runtimeConfig()

	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Int64(appLogger.KeyOrganizationID, orgID),
		zap.Stringer(appLogger.KeyIdentifierType, identifierType),
		zap.String(appLogger.KeyIdentifier, identifier),
		zap.Stringer(appLogger.KeyRole, role),
		zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
	)
	log.Debug("InviteUserToOrganization called")

	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return 0, err
	}
	if err := ensurePersonalInvitationAllowed(resolvedOrg.Organization); err != nil {
		return 0, err
	}
	if err := h.ensureOrganizationTypeCapabilityForOrganization(resolvedOrg.Organization, organizationTypeCapabilityInviteMembers); err != nil {
		return 0, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "invite_user_to_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Stringer(appLogger.KeyIdentifierType, identifierType),
			zap.String(appLogger.KeyIdentifier, identifier),
			zap.String(logkeys.Reason, "organization_type_capability_denied"),
		)
	}
	orgDepth := organizationDepth(resolvedOrg.Organization)

	targetUser, targetEmail, err := h.resolveOrganizationUserByIdentifier(ctx, identifierType, identifier, recipientResolutionDiagnostics{
		operation:           "invite_user_to_organization",
		emailLookupStage:    "resolve_target_by_email",
		usernameLookupStage: "resolve_target_by_username",
		extraFields: []zap.Field{
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
		},
	})
	if err != nil {
		return 0, err
	}

	// If user exists, check if they're already a member
	if targetUser != nil {
		userOrg, err := h.persistenceHandler.GetUserOrganization(ctx, targetUser.ID, orgID)
		if err != nil && !stdErrors.Is(err, persistence.ErrUserOrganizationNotFound) {
			return 0, appLogger.WithErrorCause(errors.ErrInternalError, err,
				zap.String(logkeys.Operation, "invite_user_to_organization"),
				zap.String(logkeys.Stage, "check_existing_membership"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyUserID, targetUser.ID),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
			)
		}
		if userOrg != nil {
			return 0, appLogger.WithErrorFields(errors.ErrUserAlreadyMember,
				zap.String(logkeys.Operation, "invite_user_to_organization"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyUserID, targetUser.ID),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
				zap.String(logkeys.Reason, "already_member"),
			)
		}
	}

	targetUserServiceModel := h.converter.ConvertPersistenceToUser(targetUser)

	// Check permission to add members

	admissionDiagnostics := membershipAdmissionDiagnostics{
		operation:        "invite_user_to_organization",
		actorID:          actorID,
		orgID:            orgID,
		actorErrorDomain: errors.ErrInternalError,
	}
	actorType, actorRole, err := h.resolveMembershipAdmissionActorAndValidateBulkCapability(ctx, *requestingUser, orgID, resolvedOrg, admissionDiagnostics)
	if err != nil {
		return 0, err
	}

	var targetUserRole proto_models.OrganizationRole
	if targetUser != nil {
		var targetUserRetrievalErr error
		_, targetUserRole, _, targetUserRetrievalErr = h.getActorOrgMembership(ctx, *targetUserServiceModel, orgID)

		if targetUserRetrievalErr != nil {
			return 0, appLogger.WithErrorCause(errors.ErrInternalError, targetUserRetrievalErr,
				zap.String(logkeys.Operation, "invite_user_to_organization"),
				zap.String(logkeys.Stage, "resolve_target_membership"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyUserID, targetUser.ID),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
			)
		}
	} else {
		targetUserRole = proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED
	}
	if targetUserRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		return 0, errors.ErrUserAlreadyMember
	}

	// For non-existent users, we can't check permission with their ID, so use 0
	var targetUserID uint64
	if targetUser != nil {
		targetUserID = targetUser.ID
	}
	targetDiagnostics := admissionDiagnostics.withTarget(common_service_models.UserIdType(targetUserID))
	canAdd, permissionCheckErr := h.modifyOtherMemberRolePermissionCheckAtDepth(ctx, actorType, actorRole, targetUserID, orgID, orgDepth, role, &targetUserRole)
	if permissionCheckErr != nil {
		return 0, targetDiagnostics.errorAtStage(permissionCheckErr, "check_role_transition_permission")
	}

	// Also verify actor has InviteMembers permission specifically
	canInvite, inviteErr := h.permissionHandler.HasOrgPermission(ctx, actorType, actorRole, permission.PermissionOrganizationInviteMembers, orgDepth)
	if inviteErr != nil {
		return 0, admissionDiagnostics.internalErrorAtStage(inviteErr, "check_invite_permission")
	}

	if !canAdd || !canInvite {
		return 0, targetDiagnostics.denied()
	}

	// Preserve strict ancestor validation, but defer auto-adds until acceptance:
	// ancestor memberships can grant inherited access while an invitation is pending.
	if targetUser != nil && !runtimeConfig.OrgAncestorMembershipAutoAdd() {
		if err := h.ensureAncestorMembershipForLineage(ctx, common_service_models.UserIdType(targetUser.ID), resolvedOrg.Organization.Lineage, runtimeConfig); err != nil {
			return 0, appLogger.WithErrorFields(err,
				zap.String(logkeys.Operation, "invite_user_to_organization"),
				zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
				zap.Uint64(appLogger.KeyUserID, targetUser.ID),
				zap.Int64(appLogger.KeyOrganizationID, orgID),
				zap.String(logkeys.Reason, "missing_ancestor_membership"),
			)
		}
	}

	// Check for existing pending invitation
	existingInvite, err := h.persistenceHandler.GetPendingInvitationByEmailAndOrg(ctx, targetEmail, orgID)
	if err != nil && !stdErrors.Is(err, persistence.ErrInvitationNotFound) {
		return 0, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "invite_user_to_organization"),
			zap.String(logkeys.Stage, "check_existing_invitation"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.String(appLogger.KeyEmail, targetEmail),
		)
	}
	if existingInvite != nil {
		return 0, appLogger.WithErrorFields(errors.ErrInvitationAlreadyExists,
			zap.String(logkeys.Operation, "invite_user_to_organization"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Uint64(appLogger.KeyInvitationID, existingInvite.ID),
			zap.String(appLogger.KeyEmail, targetEmail),
			zap.String(logkeys.Reason, "pending_invitation_exists"),
		)
	}

	// Create the invitation
	expiryDuration := runtimeConfig.OrganizationInvitationExpiryDays()
	invitation := &common_persistence_models.OrganizationInvitation{
		OrganizationID:     orgID,
		Email:              targetEmail,
		Role:               role,
		InvitedByUserID:    uint64(actorID),
		ExpiresAt:          time.Now().Add(expiryDuration),
		RequiresAcceptance: true,
	}

	err = h.persistenceHandler.CreateOrganizationInvitation(ctx, invitation)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrInvitationAlreadyExists) {
			return 0, errors.ErrInvitationAlreadyExists
		}
		return 0, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "invite_user_to_organization"),
			zap.String(logkeys.Stage, "persist_invitation"),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.String(appLogger.KeyEmail, targetEmail),
			zap.Stringer(appLogger.KeyRole, role),
		)
	}

	if h.notificationSink != nil && targetUser != nil {
		if err := h.notificationSink.OrganizationInvitationRecorded(ctx, invitation.ID, targetUser.ID); err != nil {
			return 0, err
		}
	}
	log.Info("invitation created successfully",
		zap.Uint64(appLogger.KeyInvitationID, invitation.ID),
		zap.Bool(appLogger.KeyUserExists, targetUser != nil),
	)

	return invitation.ID, nil
}

// AddUserToOrganizationByIdentifier attempts to directly add a user by identifier.
// If the user exists, they are added immediately (requires add-member permission).
func (h *OrganizationHandler[U]) AddUserToOrganizationByIdentifier(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
	identifierType proto_models.UserIdentifierType,
	identifier string,
	role proto_models.OrganizationRole,
) (*AddUserResult, error) {
	// Get actor from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, errors.ErrUserNotFound
	}
	actor := *requestingUser
	actorID := actor.GetID()

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Stringer(appLogger.KeyIdentifierType, identifierType),
			zap.String(appLogger.KeyIdentifier, identifier),
			zap.Stringer(appLogger.KeyRole, role),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
		).Debug("AddUserToOrganizationByIdentifier called")
	}

	if orgID == 0 || identifier == "" {
		return nil, errors.ErrInvalidRequest
	}

	targetUser, targetEmail, err := h.resolveOrganizationUserByIdentifier(ctx, identifierType, identifier, organizationIdentifierResolutionDiagnostics)
	if err != nil {
		return nil, err
	}

	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return nil, err
	}
	if err := ensurePersonalInvitationAllowed(resolvedOrg.Organization); err != nil {
		return nil, err
	}

	if err := h.ensureOrganizationTypeCapabilityForOrganization(resolvedOrg.Organization, organizationTypeCapabilityAddMembers); err != nil {
		return nil, appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "add_organization_member_by_identifier"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(appLogger.KeyIdentifier, identifier), zap.String(logkeys.Reason, "organization_type_capability_denied"))
	}
	orgDepth := organizationDepth(resolvedOrg.Organization)

	targetUserServiceModel := h.converter.ConvertPersistenceToUser(targetUser)

	// Check permission to add members

	admissionDiagnostics := membershipAdmissionDiagnostics{
		operation:        "add_organization_member_by_identifier",
		actorID:          actorID,
		orgID:            orgID,
		actorErrorDomain: errors.ErrInternalError,
	}
	actorType, actorRole, err := h.resolveMembershipAdmissionActorAndValidateBulkCapability(ctx, *requestingUser, orgID, resolvedOrg, admissionDiagnostics)
	if err != nil {
		return nil, err
	}

	var targetUserRole proto_models.OrganizationRole
	if targetUser != nil {
		var targetUserRetrievalErr error
		_, targetUserRole, _, targetUserRetrievalErr = h.getActorOrgMembership(ctx, *targetUserServiceModel, orgID)

		if targetUserRetrievalErr != nil {
			return nil, appLogger.WithErrorCause(errors.ErrInternalError, targetUserRetrievalErr, zap.String(logkeys.Operation, "add_organization_member_by_identifier"), zap.String(logkeys.Stage, "resolve_target_membership"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Uint64(appLogger.KeyUserID, targetUser.ID), zap.Int64(appLogger.KeyOrganizationID, orgID))
		}
	} else {
		targetUserRole = proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED
	}
	if targetUserRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		return nil, errors.ErrUserAlreadyMember
	}

	// For non-existent users, we can't check permission with their ID, so use 0
	var targetUserID uint64
	if targetUser != nil {
		targetUserID = targetUser.ID
	}
	targetDiagnostics := admissionDiagnostics.withTarget(common_service_models.UserIdType(targetUserID))
	canAdd, permissionCheckErr := h.modifyOtherMemberRolePermissionCheckAtDepth(ctx, actorType, actorRole, targetUserID, orgID, orgDepth, role, &targetUserRole)
	if permissionCheckErr != nil {
		return nil, targetDiagnostics.errorAtStage(permissionCheckErr, "check_role_transition_permission")
	}

	// Also verify actor has AddMembers permission specifically
	canAddPerm, addErr := h.permissionHandler.HasOrgPermission(ctx, actorType, actorRole, permission.PermissionOrganizationAddMembers, orgDepth)
	if addErr != nil {
		return nil, targetDiagnostics.internalErrorAtStage(addErr, "check_add_permission")
	}

	if !canAdd || !canAddPerm {
		return nil, targetDiagnostics.denied()
	}

	return h.addUserToOrganizationByIdentifierPrivilegedResolved(ctx, actorID, resolvedOrg, targetUser, targetEmail, role)
}

// AddUserToOrganizationByIdentifierPrivileged adds a user or creates an auto-add
func (h *OrganizationHandler[U]) AddUserToOrganizationByIdentifierPrivileged(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
	identifierType proto_models.UserIdentifierType,
	identifier string,
	role proto_models.OrganizationRole,
) (*AddUserResult, error) {
	return h.addUserToOrganizationByIdentifierPrivileged(
		ctx,
		h.actorUserIDFromContext(ctx),
		orgID,
		identifierType,
		identifier,
		role,
	)
}

// addUserToOrganizationByIdentifierPrivileged resolves a target user/email identifier and delegates to the shared privileged add flow used by trusted callers.
func (h *OrganizationHandler[U]) addUserToOrganizationByIdentifierPrivileged(
	ctx context.Context,
	actorID common_service_models.UserIdType,
	orgID common_service_models.OrganizationIdType,
	identifierType proto_models.UserIdentifierType,
	identifier string,
	role proto_models.OrganizationRole,
) (*AddUserResult, error) {
	if orgID == 0 || identifier == "" {
		return nil, errors.ErrInvalidRequest
	}

	targetUser, targetEmail, err := h.resolveOrganizationUserByIdentifier(ctx, identifierType, identifier, organizationIdentifierResolutionDiagnostics)
	if err != nil {
		return nil, err
	}

	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return nil, err
	}

	return h.addUserToOrganizationByIdentifierPrivilegedResolved(ctx, actorID, resolvedOrg, targetUser, targetEmail, role)
}

// addUserToOrganizationByIdentifierPrivilegedResolved executes the privileged add flow after organization and target resolution, either adding an existing user or creating an auto-add invitation.
func (h *OrganizationHandler[U]) addUserToOrganizationByIdentifierPrivilegedResolved(
	ctx context.Context,
	actorID common_service_models.UserIdType,
	resolvedOrg *resolvedOrganization,
	targetUser *common_persistence_models.User,
	targetEmail string,
	role proto_models.OrganizationRole,
) (*AddUserResult, error) {
	if resolvedOrg == nil || resolvedOrg.Organization == nil {
		return nil, errors.ErrOrganizationNotFound
	}
	if err := ensurePersonalInvitationAllowed(resolvedOrg.Organization); err != nil {
		return nil, err
	}

	orgID := resolvedOrg.Organization.ID
	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Int64(appLogger.KeyOrganizationID, orgID),
		zap.Stringer(appLogger.KeyRole, role),
		zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
	)

	// If user exists, add immediately through the shared privileged member path.
	if targetUser != nil {
		memberResp, err := h.addMemberPrivilegedWithResolvedOrg(
			ctx,
			actorID,
			common_service_models.UserIdType(targetUser.ID),
			resolvedOrg,
			role,
		)
		if err != nil {
			return nil, err
		}

		return &AddUserResult{
			UserAdded:        true,
			AutoAddPending:   false,
			OrganizationUser: memberResp,
		}, nil
	}

	expiryDuration := h.runtimeConfig().OrganizationAutoAddExpiryDays()
	autoAddEntry := &common_persistence_models.OrganizationInvitation{
		OrganizationID:     orgID,
		Email:              targetEmail,
		Role:               role,
		InvitedByUserID:    uint64(actorID),
		ExpiresAt:          time.Now().Add(expiryDuration),
		RequiresAcceptance: false,
	}

	err := h.persistenceHandler.CreateOrganizationInvitation(ctx, autoAddEntry)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrInvitationAlreadyExists) {
			return nil, errors.ErrInvitationAlreadyExists
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "add_organization_member_by_identifier"), zap.String(logkeys.Stage, "persist_auto_add_entry"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(appLogger.KeyEmail, targetEmail), zap.Stringer(appLogger.KeyRole, role))
	}

	log.Info("auto-add entry created for non-existent user",
		zap.Uint64(appLogger.KeyInvitationID, autoAddEntry.ID),
	)

	return &AddUserResult{
		UserAdded:      false,
		AutoAddPending: true,
		AutoAddEntry:   h.converter.PersistenceOrganizationInvitationToService(autoAddEntry),
	}, nil
}

// GetPendingInvitations retrieves all pending invitations for the authenticated user.
func (h *OrganizationHandler[U]) GetPendingInvitations(
	ctx context.Context,
) ([]*common_service_models.OrganizationInvitation, error) {
	// Get user from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, errors.ErrUserNotFound
	}
	userEmail := (*requestingUser).GetEmail()

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.String(appLogger.KeyEmail, userEmail),
		).Debug("GetPendingInvitations called")
	}

	// Get pending invitations from persistence
	persistenceInvitations, err := h.persistenceHandler.GetPendingInvitationsByEmail(ctx, userEmail)
	if err != nil {
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "get_pending_organization_invitations"), zap.String(logkeys.Stage, "load_invitations"), zap.Uint64(appLogger.KeyUserID, uint64((*requestingUser).GetID())), zap.String(appLogger.KeyEmail, userEmail))
	}

	// Convert to service models
	serviceInvitations := make([]*common_service_models.OrganizationInvitation, len(persistenceInvitations))
	for i, pInvite := range persistenceInvitations {
		serviceInvitations[i] = h.converter.PersistenceOrganizationInvitationToService(pInvite)
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.String(appLogger.KeyEmail, userEmail),
		).Debug("retrieved pending invitations", zap.Int(appLogger.KeyCount, len(serviceInvitations)))
	}
	return serviceInvitations, nil
}

// GetPendingInvitationsPage is the bounded API-facing pending-invitation list.
// GetPendingInvitations remains the full-list helper for internal workflows.
func (h *OrganizationHandler[U]) GetPendingInvitationsPage(
	ctx context.Context,
	pagination *proto_models.PaginationRequest,
) ([]*common_service_models.OrganizationInvitation, *proto_models.PaginationResponse, error) {
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, nil, errors.ErrUserNotFound
	}
	userEmail := (*requestingUser).GetEmail()
	persistenceInvitations, paginationResponse, err := h.persistenceHandler.GetPendingInvitationsByEmailPage(ctx, userEmail, pagination)
	if err != nil {
		return nil, nil, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "get_pending_organization_invitations_page"), zap.String(logkeys.Stage, "load_invitations"), zap.Uint64(appLogger.KeyUserID, uint64((*requestingUser).GetID())), zap.String(appLogger.KeyEmail, userEmail), zap.Uint32(logkeys.Limit, pagination.GetLimit()))
	}

	serviceInvitations := make([]*common_service_models.OrganizationInvitation, 0, len(persistenceInvitations))
	for _, invitation := range persistenceInvitations {
		if invitation == nil {
			continue
		}
		serviceInvitations = append(serviceInvitations, h.converter.PersistenceOrganizationInvitationToService(invitation))
	}
	return serviceInvitations, paginationResponse, nil
}

// resolveInvitationOrganization reuses the active organization preloaded by
// invitation persistence and falls back to an ordinary active lookup for mocks
// or custom persistence implementations that omit the association.
func (h *OrganizationHandler[U]) resolveInvitationOrganization(
	ctx context.Context,
	invitation *common_persistence_models.OrganizationInvitation,
) (*resolvedOrganization, error) {
	if invitation != nil &&
		invitation.Organization.ID == invitation.OrganizationID &&
		!invitation.Organization.DeletedAt.Valid {
		return &resolvedOrganization{
			Organization: h.converter.PersistenceOrganizationToService(&invitation.Organization),
		}, nil
	}

	if invitation == nil {
		return nil, errors.ErrOrganizationNotFound
	}
	return h.resolveOrganization(ctx, common_service_models.OrganizationIdType(invitation.OrganizationID))
}

func (h *OrganizationHandler[U]) loadPendingInvitationForUser(
	ctx context.Context,
	invitationID uint64,
	userID common_service_models.UserIdType,
	email string,
	operation string,
) (*common_persistence_models.OrganizationInvitation, error) {
	invitation, err := h.persistenceHandler.GetOrganizationInvitationByID(ctx, invitationID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrInvitationNotFound) {
			return nil, errors.ErrInvitationNotFound
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, operation),
			zap.String(logkeys.Stage, "load_invitation"),
			zap.Uint64(appLogger.KeyInvitationID, invitationID),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyEmail, email),
		)
	}
	// Check the subject first so another user cannot discover processed state.
	if !strings.EqualFold(invitation.Email, email) {
		return nil, appLogger.WithErrorFields(errors.ErrInvitationNotFound,
			zap.String(logkeys.Operation, operation),
			zap.Uint64(appLogger.KeyInvitationID, invitationID),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyEmail, email),
			zap.Int64(appLogger.KeyOrganizationID, invitation.OrganizationID),
			zap.String(logkeys.Reason, "invitation_subject_mismatch"),
		)
	}
	if invitation.AcceptedAt != nil || invitation.DeclinedAt != nil {
		return nil, appLogger.WithErrorFields(errors.ErrInvitationAlreadyProcessed,
			zap.String(logkeys.Operation, operation),
			zap.Uint64(appLogger.KeyInvitationID, invitationID),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, invitation.OrganizationID),
			zap.String(logkeys.Reason, "already_processed"),
		)
	}
	return invitation, nil
}

// AcceptInvitation accepts a pending invitation and adds the authenticated user to the target organization.
func (h *OrganizationHandler[U]) AcceptInvitation(
	ctx context.Context,
	invitationID uint64,
) (*common_service_models.UserOrganizationResponse, error) {
	if err := h.requireInvitationNotificationTransaction(ctx); err != nil {
		return nil, err
	}
	// Get user from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, errors.ErrUserNotFound
	}
	userID := (*requestingUser).GetID()
	userEmail := (*requestingUser).GetEmail()

	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Uint64(appLogger.KeyInvitationID, invitationID),
		zap.Uint64(appLogger.KeyUserID, uint64(userID)),
	)
	log.Debug("AcceptInvitation called")

	invitation, err := h.loadPendingInvitationForUser(ctx, invitationID, userID, userEmail, "accept_organization_invitation")
	if err != nil {
		return nil, err
	}
	invitationFields := func(extra ...zap.Field) []zap.Field {
		return append([]zap.Field{
			zap.String(logkeys.Operation, "accept_organization_invitation"),
			zap.Uint64(appLogger.KeyInvitationID, invitationID),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, invitation.OrganizationID),
		}, extra...)
	}

	// Check if invitation has expired
	if invitation.IsExpired() {
		return nil, appLogger.WithErrorFields(errors.ErrInvitationExpired,
			invitationFields(zap.String(logkeys.Reason, "expired"))...)
	}

	resolvedOrg, err := h.resolveInvitationOrganization(ctx, invitation)
	if err != nil {
		return nil, err
	}
	if err := ensurePersonalInvitationAllowed(resolvedOrg.Organization); err != nil {
		return nil, err
	}

	// Atomically claim the invitation before adding any memberships so a concurrent
	// decline (or second accept) cannot overwrite the outcome; the losing request
	// gets ErrInvitationAlreadyProcessed instead.
	if err := h.persistenceHandler.MarkOrganizationInvitationAccepted(ctx, invitationID); err != nil {
		if stdErrors.Is(err, persistence.ErrInvitationNotFound) {
			return nil, errors.ErrInvitationNotFound
		}
		if stdErrors.Is(err, persistence.ErrInvitationAlreadyProcessed) {
			return nil, appLogger.WithErrorFields(errors.ErrInvitationAlreadyProcessed,
				invitationFields(zap.String(logkeys.Reason, "concurrent_processing"))...)
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			invitationFields(zap.String(logkeys.Stage, "claim_invitation"))...)
	}

	if err := h.ensureAncestorMembershipForLineage(ctx, userID, resolvedOrg.Organization.Lineage, h.runtimeConfig()); err != nil {
		return nil, appLogger.WithErrorFields(err,
			invitationFields(zap.String(logkeys.Reason, "missing_ancestor_membership"))...)
	}

	// Add user to organization
	err = h.persistenceHandler.AddUserToOrganization(ctx, uint64(userID), invitation.OrganizationID, invitation.Role)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrUserAlreadyInOrganization) {
			return nil, errors.ErrUserAlreadyMember
		}
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			invitationFields(
				zap.String(logkeys.Stage, "persist_membership"),
				zap.Stringer(appLogger.KeyRole, invitation.Role),
			)...)
	}

	// Invalidate role cache
	h.InvalidateRoleCacheForDescendants(ctx, userID, common_service_models.OrganizationIdType(invitation.OrganizationID))

	// Get the membership to return
	membership, err := h.persistenceHandler.GetUserOrganization(ctx, uint64(userID), invitation.OrganizationID)
	if err != nil {
		return nil, appLogger.InvariantError(
			"accepted_invitation_membership_readable",
			appLogger.WithErrorCause(errors.ErrInternalError, err),
			invitationFields(
				zap.String(logkeys.Stage, "reload_membership"),
				zap.String(logkeys.Expected, "accepted invitation membership can be reloaded"),
				zap.String(logkeys.Actual, "membership reload failed"),
			)...,
		)
	}

	serviceUserOrg := h.converter.PersistenceUserOrganizationToService(membership)
	if serviceUserOrg.EffectiveRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		depth := uint32(organizationDepth(resolvedOrg.Organization))
		serviceUserOrg.EffectiveRoleDepth = &depth
	}
	h.applyMembershipRoleProviderOverride(ctx, serviceUserOrg, userID, common_service_models.OrganizationIdType(invitation.OrganizationID))
	response := &common_service_models.UserOrganizationResponse{
		UserOrganization: serviceUserOrg,
	}

	if err := h.invokeOnOrganizationMemberAdded(ctx, serviceUserOrg); err != nil {
		return nil, appLogger.WithErrorFields(
			fmt.Errorf("%w: %w", errors.ErrOrganizationCallbackFailed, err),
			invitationFields(zap.String(logkeys.Stage, "member_added_callback"))...,
		)
	}

	if err := h.resolveInvitationNotification(ctx, invitationID); err != nil {
		return nil, err
	}

	log.Info("invitation accepted successfully",
		zap.Int64(appLogger.KeyOrganizationID, invitation.OrganizationID),
	)
	return response, nil
}

// DeclineInvitation declines a pending invitation for the authenticated user.
func (h *OrganizationHandler[U]) DeclineInvitation(
	ctx context.Context,
	invitationID uint64,
) error {
	if err := h.requireInvitationNotificationTransaction(ctx); err != nil {
		return err
	}
	// Get user from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return errors.ErrUserNotFound
	}
	userID := (*requestingUser).GetID()
	userEmail := (*requestingUser).GetEmail()

	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Uint64(appLogger.KeyInvitationID, invitationID),
		zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		zap.String(appLogger.KeyEmail, userEmail),
	)
	log.Debug("DeclineInvitation called")

	invitation, err := h.loadPendingInvitationForUser(ctx, invitationID, userID, userEmail, "decline_organization_invitation")
	if err != nil {
		return err
	}
	invitationFields := func(extra ...zap.Field) []zap.Field {
		return append([]zap.Field{
			zap.String(logkeys.Operation, "decline_organization_invitation"),
			zap.Uint64(appLogger.KeyInvitationID, invitationID),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int64(appLogger.KeyOrganizationID, invitation.OrganizationID),
		}, extra...)
	}

	// Atomically mark the invitation as declined; a concurrent accept wins and this
	// request gets ErrInvitationAlreadyProcessed instead of overwriting the outcome.
	err = h.persistenceHandler.MarkOrganizationInvitationDeclined(ctx, invitationID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrInvitationNotFound) {
			return errors.ErrInvitationNotFound
		}
		if stdErrors.Is(err, persistence.ErrInvitationAlreadyProcessed) {
			return appLogger.WithErrorFields(errors.ErrInvitationAlreadyProcessed,
				invitationFields(zap.String(logkeys.Reason, "concurrent_processing"))...)
		}
		return appLogger.WithErrorCause(errors.ErrInternalError, err,
			invitationFields(zap.String(logkeys.Stage, "claim_invitation"))...)
	}

	if err := h.resolveInvitationNotification(ctx, invitationID); err != nil {
		return err
	}

	log.Info("invitation declined successfully",
		zap.Int64(appLogger.KeyOrganizationID, invitation.OrganizationID),
	)
	return nil
}

// ProcessPendingInvitationsForUser processes any pending invitations for a newly registered user.
// This should be called after user registration completes.
func (h *OrganizationHandler[U]) ProcessPendingInvitationsForUser(
	ctx context.Context,
	userID common_service_models.UserIdType,
	email string,
) ([]*common_service_models.OrganizationInvitation, error) {
	if err := h.requireInvitationNotificationTransaction(ctx); err != nil {
		return nil, err
	}
	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		zap.String(appLogger.KeyEmail, email),
	)
	log.Debug("ProcessPendingInvitationsForUser called")

	// Get pending invitations that require acceptance
	persistenceInvitations, err := h.persistenceHandler.GetPendingInvitationsByEmail(ctx, email)
	if err != nil {
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "process_pending_organization_invitations"),
			zap.String(logkeys.Stage, "load_acceptance_required_invitations"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyEmail, email),
		)
	}

	// Get pending auto-add entries (requires_acceptance = false)
	autoAddInvitations, err := h.persistenceHandler.GetPendingAutoAddEntriesByEmail(ctx, email)
	if err != nil {
		return nil, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "process_pending_organization_invitations"),
			zap.String(logkeys.Stage, "load_auto_add_entries"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyEmail, email),
		)
	}

	callbacks := h.organizationCallbacks()
	callMemberAdded := callbacks != nil && callbacks.OnOrganizationMemberAdded != nil
	runtimeConfig := h.runtimeConfig()

	// Auto-accept auto-add entries by adding the user immediately
	for _, autoAdd := range autoAddInvitations {
		// Skip expired entries defensively
		if autoAdd.IsExpired() {
			continue
		}

		resolvedOrg, resolveErr := h.resolveInvitationOrganization(ctx, autoAdd)
		if resolveErr != nil {
			log.Warn("failed to resolve organization for auto-add entry", zap.Error(resolveErr),
				zap.String(logkeys.Operation, "process_pending_organization_invitations"),
				zap.String(logkeys.Stage, "resolve_auto_add_organization"),
				zap.Uint64(appLogger.KeyInvitationID, autoAdd.ID),
				zap.Int64(appLogger.KeyOrganizationID, autoAdd.OrganizationID),
				zap.String(logkeys.Fallback, "skip_auto_add_entry"))
			continue
		}
		if policyErr := ensurePersonalInvitationAllowed(resolvedOrg.Organization); policyErr != nil {
			log.Warn("skipping auto-add entry for personal workspace", zap.Error(policyErr),
				zap.String(logkeys.Operation, "process_pending_organization_invitations"),
				zap.String(logkeys.Stage, "validate_auto_add_policy"),
				zap.Uint64(appLogger.KeyInvitationID, autoAdd.ID),
				zap.Int64(appLogger.KeyOrganizationID, autoAdd.OrganizationID),
				zap.String(logkeys.Fallback, "skip_auto_add_entry"))
			continue
		}

		if err := h.ensureAncestorMembershipForLineage(ctx, userID, resolvedOrg.Organization.Lineage, runtimeConfig); err != nil {
			log.Warn("user missing ancestor memberships for auto-add entry", zap.Error(err),
				zap.String(logkeys.Operation, "process_pending_organization_invitations"),
				zap.String(logkeys.Stage, "validate_ancestor_memberships"),
				zap.Uint64(appLogger.KeyInvitationID, autoAdd.ID),
				zap.Int64(appLogger.KeyOrganizationID, autoAdd.OrganizationID),
				zap.String(logkeys.Fallback, "skip_auto_add_entry"))
			continue
		}

		// Add user to organization with the specified role
		added := false
		err := h.persistenceHandler.AddUserToOrganization(ctx, uint64(userID), autoAdd.OrganizationID, autoAdd.Role)
		if err != nil {
			if stdErrors.Is(err, persistence.ErrUserAlreadyInOrganization) {
				// Already added; continue
			} else {
				log.Error("failed to auto-add user from pending entry", zap.Error(err),
					zap.String(logkeys.Operation, "process_pending_organization_invitations"),
					zap.String(logkeys.Stage, "persist_membership"),
					zap.Uint64(appLogger.KeyInvitationID, autoAdd.ID),
					zap.Int64(appLogger.KeyOrganizationID, autoAdd.OrganizationID),
					zap.Stringer(appLogger.KeyRole, autoAdd.Role),
					zap.String(logkeys.Fallback, "skip_auto_add_entry"))
				continue
			}
		} else {
			added = true
		}

		// Mark auto-add entry as accepted; a concurrent processing run winning the
		// guarded update is fine since the membership add above is idempotent.
		err = h.persistenceHandler.MarkOrganizationInvitationAccepted(ctx, autoAdd.ID)
		if err != nil && !stdErrors.Is(err, persistence.ErrInvitationAlreadyProcessed) {
			log.Error("failed to mark auto-add entry accepted",
				zap.String(logkeys.Operation, "process_pending_organization_invitations"),
				zap.String(logkeys.Stage, "mark_auto_add_accepted"),
				zap.Uint64(appLogger.KeyInvitationID, autoAdd.ID),
				zap.Int64(appLogger.KeyOrganizationID, autoAdd.OrganizationID),
				zap.String(logkeys.Fallback, "membership_retained_with_pending_entry"),
				zap.Error(err))
		}

		if callMemberAdded && added {
			membership, membershipErr := h.persistenceHandler.GetUserOrganization(ctx, uint64(userID), autoAdd.OrganizationID)
			if membershipErr != nil {
				return nil, appLogger.InvariantError(
					"auto_added_organization_membership_readable",
					appLogger.WithErrorCause(errors.ErrInternalError, membershipErr),
					zap.String(logkeys.Operation, "process_pending_organization_invitations"),
					zap.String(logkeys.Stage, "reload_membership_for_callback"),
					zap.Uint64(appLogger.KeyInvitationID, autoAdd.ID),
					zap.Uint64(appLogger.KeyUserID, uint64(userID)),
					zap.Int64(appLogger.KeyOrganizationID, autoAdd.OrganizationID),
					zap.String(logkeys.Expected, "auto-added membership can be reloaded"),
					zap.String(logkeys.Actual, "membership reload failed"),
				)
			}

			serviceUserOrg := h.converter.PersistenceUserOrganizationToService(membership)
			if err := h.invokeOnOrganizationMemberAdded(ctx, serviceUserOrg); err != nil {
				return nil, appLogger.WithErrorFields(
					fmt.Errorf("%w: %w", errors.ErrOrganizationCallbackFailed, err),
					zap.String(logkeys.Operation, "process_pending_organization_invitations"),
					zap.String(logkeys.Stage, "member_added_callback"),
					zap.Uint64(appLogger.KeyInvitationID, autoAdd.ID),
					zap.Uint64(appLogger.KeyUserID, uint64(userID)),
					zap.Int64(appLogger.KeyOrganizationID, autoAdd.OrganizationID),
				)
			}
		}

		h.InvalidateRoleCacheForDescendants(ctx, userID, common_service_models.OrganizationIdType(autoAdd.OrganizationID))
	}

	if len(persistenceInvitations) == 0 {
		log.Debug("no pending invitations found for new user")
		return nil, nil
	}

	// Convert to service models (only acceptance-required invitations are returned)
	serviceInvitations := make([]*common_service_models.OrganizationInvitation, len(persistenceInvitations))
	for i, pInvite := range persistenceInvitations {
		serviceInvitations[i] = h.converter.PersistenceOrganizationInvitationToService(pInvite)
	}

	if err := h.NotifyPendingInvitationsForUser(ctx, userID, email); err != nil {
		return nil, err
	}
	if ce := log.Check(zap.DebugLevel, "found pending invitations for new user"); ce != nil {
		ce.Write(
			zap.Int(appLogger.KeyCount, len(serviceInvitations)),
		)
	}

	return serviceInvitations, nil
}

// CancelInvitation cancels a pending invitation (admin action).
func (h *OrganizationHandler[U]) CancelInvitation(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
	invitationID uint64,
) error {
	if err := h.requireInvitationNotificationTransaction(ctx); err != nil {
		return err
	}
	// Get actor from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return errors.ErrUserNotFound
	}
	actor := *requestingUser
	actorID := actor.GetID()

	log := appLogger.For(ctx, h.logger, organizationLoggerName).With(
		zap.Int64(appLogger.KeyOrganizationID, orgID),
		zap.Uint64(appLogger.KeyInvitationID, invitationID),
		zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
	)
	log.Debug("CancelInvitation called")

	if err := h.ensureOrganizationTypeCapability(ctx, orgID, organizationTypeCapabilityCancelInvitation); err != nil {
		return appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "cancel_organization_invitation"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Uint64(appLogger.KeyInvitationID, invitationID), zap.String(logkeys.Reason, "organization_type_capability_denied"))
	}
	actorType, actorRole, _, err := h.getActorOrgMembership(ctx, *requestingUser, orgID)
	if err != nil {
		return appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "cancel_organization_invitation"), zap.String(logkeys.Stage, "resolve_actor_membership"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Uint64(appLogger.KeyInvitationID, invitationID))
	}
	orgDepth, depthErr := h.getOrgDepthMapped(ctx, orgID)
	if depthErr != nil {
		return depthErr
	}

	// Check permission to cancel invitations
	canCancel, err := h.permissionHandler.HasOrgPermission(
		ctx,
		actorType,
		actorRole,
		permission.PermissionOrganizationCancelInvitation,
		orgDepth,
	)
	if err != nil {
		return appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "cancel_organization_invitation"), zap.String(logkeys.Stage, "permission_check"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Uint64(appLogger.KeyInvitationID, invitationID))
	}
	if !canCancel {
		return appLogger.WithErrorFields(errors.ErrInsufficientPermissions, zap.String(logkeys.Operation, "cancel_organization_invitation"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Uint64(appLogger.KeyInvitationID, invitationID), zap.String(logkeys.Reason, "insufficient_permission"))
	}

	// Get the invitation to verify it exists and belongs to this org
	invitation, err := h.persistenceHandler.GetOrganizationInvitationByID(ctx, invitationID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrInvitationNotFound) {
			return errors.ErrInvitationNotFound
		}
		return appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "cancel_organization_invitation"), zap.String(logkeys.Stage, "load_invitation"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Uint64(appLogger.KeyInvitationID, invitationID))
	}

	// Verify the invitation belongs to this organization
	if invitation.OrganizationID != orgID {
		return appLogger.WithErrorFields(errors.ErrInvitationNotFound, zap.String(logkeys.Operation, "cancel_organization_invitation"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Uint64(appLogger.KeyInvitationID, invitationID), zap.String(logkeys.Reason, "organization_mismatch"))
	}

	// Check if invitation is still pending
	if invitation.AcceptedAt != nil || invitation.DeclinedAt != nil {
		return appLogger.WithErrorFields(errors.ErrInvitationAlreadyProcessed, zap.String(logkeys.Operation, "cancel_organization_invitation"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Uint64(appLogger.KeyInvitationID, invitationID), zap.String(logkeys.Reason, "already_processed"))
	}

	// Delete the invitation; the pending guard rejects cancelling an invitation a
	// concurrent request already accepted or declined.
	err = h.persistenceHandler.DeleteOrganizationInvitation(ctx, invitationID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrInvitationNotFound) {
			return errors.ErrInvitationNotFound
		}
		if stdErrors.Is(err, persistence.ErrInvitationAlreadyProcessed) {
			return appLogger.WithErrorFields(errors.ErrInvitationAlreadyProcessed, zap.String(logkeys.Operation, "cancel_organization_invitation"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Uint64(appLogger.KeyInvitationID, invitationID), zap.String(logkeys.Reason, "concurrent_processing"))
		}
		return appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "cancel_organization_invitation"), zap.String(logkeys.Stage, "delete_invitation"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Uint64(appLogger.KeyInvitationID, invitationID))
	}

	if err := h.resolveInvitationNotification(ctx, invitationID); err != nil {
		return err
	}

	log.Info("invitation cancelled successfully",
		zap.String(appLogger.KeyEmail, invitation.Email),
	)
	return nil
}

// ListOrganizationInvitations retrieves all pending invitations for an organization.
func (h *OrganizationHandler[U]) ListOrganizationInvitations(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
	limit, offset int,
) ([]*common_service_models.OrganizationInvitation, int64, error) {
	// Get actor from context
	requestingUser, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, 0, errors.ErrUserNotFound
	}
	actor := *requestingUser
	actorID := actor.GetID()

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Int(logkeys.Limit, limit),
			zap.Int(appLogger.KeyOffset, offset),
		).Debug("ListOrganizationInvitations called")
	}

	if err := h.ensureOrganizationTypeCapability(ctx, orgID, organizationTypeCapabilityViewInvitations); err != nil {
		return nil, 0, appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "list_organization_invitations"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(logkeys.Reason, "organization_type_capability_denied"))
	}

	actorType, actorRole, _, err := h.getActorOrgMembership(ctx, *requestingUser, orgID)
	if err != nil {
		return nil, 0, appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "list_organization_invitations"), zap.String(logkeys.Stage, "resolve_actor_membership"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID))
	}
	orgDepth, depthErr := h.getOrgDepthMapped(ctx, orgID)
	if depthErr != nil {
		return nil, 0, depthErr
	}
	// Check permission to view invitations
	canView, err := h.permissionHandler.HasOrgPermission(
		ctx,
		actorType, actorRole,
		permission.PermissionOrganizationViewInvitations,
		orgDepth,
	)
	if err != nil {
		return nil, 0, appLogger.WithErrorFields(err, zap.String(logkeys.Operation, "list_organization_invitations"), zap.String(logkeys.Stage, "permission_check"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID))
	}
	if !canView {
		return nil, 0, appLogger.WithErrorFields(errors.ErrInsufficientPermissions, zap.String(logkeys.Operation, "list_organization_invitations"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.String(logkeys.Reason, "insufficient_permission"))
	}

	// Get invitations from persistence
	persistenceInvitations, total, err := h.persistenceHandler.GetPendingInvitationsByOrganization(ctx, orgID, limit, offset)
	if err != nil {
		return nil, 0, appLogger.WithErrorCause(errors.ErrInternalError, err, zap.String(logkeys.Operation, "list_organization_invitations"), zap.String(logkeys.Stage, "load_invitations"), zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)), zap.Int64(appLogger.KeyOrganizationID, orgID), zap.Int(logkeys.Limit, limit), zap.Int(appLogger.KeyOffset, offset))
	}

	// Convert to service models
	serviceInvitations := make([]*common_service_models.OrganizationInvitation, len(persistenceInvitations))
	for i, pInvite := range persistenceInvitations {
		serviceInvitations[i] = h.converter.PersistenceOrganizationInvitationToService(pInvite)
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, organizationLoggerName).With(
			zap.Int64(appLogger.KeyOrganizationID, orgID),
			zap.Uint64(appLogger.KeyActorUserID, uint64(actorID)),
			zap.Int(logkeys.Limit, limit),
			zap.Int(appLogger.KeyOffset, offset),
		).Debug("retrieved organization invitations",
			zap.Int(appLogger.KeyCount, len(serviceInvitations)),
			zap.Int64(appLogger.KeyTotal, total),
		)
	}
	return serviceInvitations, total, nil
}
