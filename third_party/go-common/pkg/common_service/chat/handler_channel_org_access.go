package chat

import (
	"context"
	"errors"
	"fmt"
	"slices"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// validOrganizationReadMinRole reports whether role can serve as a channel's minimum organization
// role for read access: a value defined by the OrganizationRole enum other than UNSPECIFIED, which
// is reserved for "no org-derived read access".
func validOrganizationReadMinRole(role proto_models.OrganizationRole) bool {
	if role == proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		return false
	}
	_, defined := proto_models.OrganizationRole_name[int32(role)]
	return defined
}

// organizationRoleAtLeast reports whether role meets minimum. OrganizationRole is an ordered enum
// whose numeric value rises with privilege (MEMBER < MODERATOR < ADMIN < OWNER, tiers ascending
// within each family), and go-common has no exported comparison helper: the organization package
// compares numerically too (organization/permission_handler.go, `userRole >= requiredRole`). The
// same semantics are repeated here so the chat package keeps depending on the
// ports.OrganizationRoleResolver seam only, not on the organization package.
func organizationRoleAtLeast(role, minimum proto_models.OrganizationRole) bool {
	return role >= minimum
}

// channelOrgReadScope returns the organization a channel grants read access to, or false when the
// channel names no organization or no valid minimum read role.
func channelOrgReadScope(channel *common_persistence_models.ChatChannel) (common_service_models.OrganizationIdType, bool) {
	if channel == nil || channel.OrganizationID == nil || *channel.OrganizationID <= 0 || !validOrganizationReadMinRole(channel.OrgMinRoleRead) {
		return 0, false
	}
	return common_service_models.OrganizationIdType(*channel.OrganizationID), true
}

// organizationReadGranted applies a channel's own organization access to a user who is not a
// participant. It grants only when the channel names an organization and a minimum read role, a
// resolver is wired, and the user is an effective member of that organization whose effective
// (lineage-aware) role meets the minimum. The channel row comes from the access cache and the
// role from the resolver's own cache, so nothing is cached here. A resolver failure is propagated
// wrapped; it is never read as "not granted".
func (h *Handler[U]) organizationReadGranted(
	ctx context.Context,
	channel *common_persistence_models.ChatChannel,
	userID common_service_models.UserIdType,
) (bool, error) {
	organizationID, ok := channelOrgReadScope(channel)
	if h.organizationRoleResolver == nil || !ok {
		return false, nil
	}
	role, isMember, err := h.organizationRoleResolver.EffectiveOrganizationRole(ctx, userID, organizationID)
	if err != nil {
		return false, fmt.Errorf("chat channel organization role check failed: %w", err)
	}
	return isMember && organizationRoleAtLeast(role, channel.OrgMinRoleRead), nil
}

// validateChannelOrganizationAccess checks a non-nil access value before it is persisted. Both
// failures wrap ErrInvalidRequest so the API layer keeps mapping them to 400.
func validateChannelOrganizationAccess(access *common_service_models.ChatChannelOrganizationAccess) error {
	if access.OrganizationID <= 0 {
		return fmt.Errorf("%w: channel organization access requires a positive OrganizationID", serviceErrors.ErrInvalidRequest)
	}
	if !validOrganizationReadMinRole(access.MinRoleRead) {
		return fmt.Errorf("%w: channel organization access requires a defined, non-UNSPECIFIED MinRoleRead; pass nil to clear access", serviceErrors.ErrInvalidRequest)
	}
	return nil
}

// organizationReadMinRoleOrDefault returns role, or, when role is UNSPECIFIED, the byDepth entry
// (org_min_role_read_agent_transcript) at organizationID's hierarchy depth. It keeps UNSPECIFIED
// without a role resolver, where no access could ever grant, and reads the organization only when
// some depth names a role.
func (h *Handler[U]) organizationReadMinRoleOrDefault(
	ctx context.Context,
	byDepth []proto_models.OrganizationRole,
	organizationID common_service_models.OrganizationIdType,
	role proto_models.OrganizationRole,
) (proto_models.OrganizationRole, error) {
	unspecified := proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED
	if role != unspecified || h.organizationRoleResolver == nil ||
		!slices.ContainsFunc(byDepth, func(r proto_models.OrganizationRole) bool { return r != unspecified }) {
		return role, nil
	}
	organization, err := h.persistenceHandler.GetOrganizationByID(ctx, int64(organizationID))
	if err != nil {
		if errors.Is(err, persistence.ErrOrganizationNotFound) {
			return unspecified, fmt.Errorf("%w: organization %d not found", serviceErrors.ErrInvalidRequest, organizationID)
		}
		if errors.Is(err, persistence.ErrDatabaseError) {
			return unspecified, serviceErrors.ErrDatabaseError
		}
		return unspecified, serviceErrors.ErrInternalError
	}
	if depth := len(organization.Lineage); depth < len(byDepth) {
		return byDepth[depth], nil
	}
	return unspecified, nil
}

// grantNewChannelOrganizationRead stores organization read access on a channel created in the
// current transaction and reflects it on created. Only server-side creates reach it, so the
// creator need not belong to the organization: the caller has established its authority there.
func (h *Handler[U]) grantNewChannelOrganizationRead(
	ctx context.Context,
	created *common_persistence_models.ChatChannel,
	organizationID common_service_models.OrganizationIdType,
	minRoleRead proto_models.OrganizationRole,
) error {
	id := int64(organizationID)
	if err := h.persistenceHandler.SetChatChannelOrganizationAccess(ctx, created.ID, &id, minRoleRead); err != nil {
		return channelAccessWriteError(err, persistence.ErrChatChannelOrganizationAccessInvalid)
	}
	created.OrganizationID = &id
	created.OrgMinRoleRead = minRoleRead
	h.invalidateChannelAccessCache(ctx, created.ID)
	return nil
}

// SetChannelOrganizationAccess sets or clears the organization access a channel owns. With a
// non-nil access, every effective member of access.OrganizationID whose effective role is at least
// access.MinRoleRead may read the channel (channel, participants, messages, reactions, edit
// history, usage and agent-session metadata) without being a participant, including after the
// channel is archived. Effective roles are lineage-aware: when organization role inheritance is
// enabled, a qualifying role held in an ancestor organization counts. nil clears the access, so
// only participants can read again.
//
// The grant is read-only. Organization readers are not participants: they are not added to the
// roster, do not see the channel in ListChannels, and every
// mutation still requires membership. The ordinary Authorize callback still runs for each read, so
// it remains the way to deny an operation.
//
// The caller must be the channel owner (archived channels are allowed), and when setting access
// must itself be an effective member of access.OrganizationID, so an owner cannot expose a channel
// to an organization it does not belong to. Authorize is consulted with
// ChatPermissionUpdateChannel. Invalid input returns an error wrapping ErrInvalidRequest; a
// non-owner or non-member caller gets ErrInsufficientPermissions. Requires an ambient transaction.
func (h *Handler[U]) SetChannelOrganizationAccess(
	ctx context.Context,
	channelID uint64,
	access *common_service_models.ChatChannelOrganizationAccess,
) error {
	if h.chatDisabled() {
		return serviceErrors.ErrChatDisabled
	}
	if access != nil {
		if err := validateChannelOrganizationAccess(access); err != nil {
			return err
		}
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return err
	}
	if err := h.requireTransaction(ctx); err != nil {
		return err
	}
	channel, err := h.requireChannelOwner(ctx, channelID, actorID, true)
	if err != nil {
		return err
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionUpdateChannel,
		Actor:     actor,
		Channel:   channel,
	}); err != nil {
		return err
	}

	var organizationID *int64
	minRoleRead := proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED
	if access != nil {
		if err := h.requireOrganizationAccessMember(ctx, actorID, access.OrganizationID); err != nil {
			return err
		}
		id := int64(access.OrganizationID)
		organizationID = &id
		minRoleRead = access.MinRoleRead
	}

	if setErr := h.persistenceHandler.SetChatChannelOrganizationAccess(ctx, channelID, organizationID, minRoleRead); setErr != nil {
		return channelAccessWriteError(setErr, persistence.ErrChatChannelOrganizationAccessInvalid)
	}

	// The access rule is read from the cached channel row, so the change must evict it (now and
	// again after commit) or reads keep following the previous rule until the cache TTL.
	h.invalidateChannelAccessCache(ctx, channelID)
	h.invalidateRealtimeAfterCommit(ctx, channelID)
	return nil
}

// channelAccessWriteError maps a channel access write's persistence error to the service error;
// invalid is the persistence sentinel for input the write rejects.
func channelAccessWriteError(err, invalid error) error {
	switch {
	case errors.Is(err, persistence.ErrChatChannelNotFound):
		return serviceErrors.ErrChatChannelNotFound
	case errors.Is(err, invalid):
		return fmt.Errorf("%w: %v", serviceErrors.ErrInvalidRequest, err)
	case errors.Is(err, persistence.ErrDatabaseError):
		return serviceErrors.ErrDatabaseError
	default:
		return serviceErrors.ErrInternalError
	}
}

// requireOrganizationAccessMember lets actorID give a channel it owns organization access only as
// an effective member of the organization: ErrInsufficientPermissions otherwise, and
// ErrInvalidRequest without an organization role resolver.
func (h *Handler[U]) requireOrganizationAccessMember(ctx context.Context, actorID uint64, organizationID common_service_models.OrganizationIdType) error {
	if h.organizationRoleResolver == nil {
		// Without a resolver the access could never grant a read, and the caller's own
		// organization membership cannot be established.
		return fmt.Errorf("%w: channel organization access requires organizations to be enabled", serviceErrors.ErrInvalidRequest)
	}
	_, isMember, err := h.organizationRoleResolver.EffectiveOrganizationRole(ctx, actorID, organizationID)
	if err != nil {
		return fmt.Errorf("chat channel organization role check failed: %w", err)
	}
	if !isMember {
		return serviceErrors.ErrInsufficientPermissions
	}
	return nil
}
