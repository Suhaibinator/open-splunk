package organization

import (
	"context"
	stdErrors "errors"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
)

// lockPersonalWorkspaceParentForCreate serializes personal-tree growth with
// account archival using the lifecycle-wide user -> root -> parent lock order.
// The owner-row lock is FOR NO KEY UPDATE, which conflicts with itself, so
// creation serializes against lifecycle peers (ensure, archive, restore) that
// take the same lock on the same row; the child's ownership foreign key only
// takes FOR KEY SHARE, which does not conflict with NO KEY UPDATE and needs
// no ordering. A deeper parent is reloaded directly so an archive that won
// the race is observed before any child insert occurs.
func (h *OrganizationHandler[U]) lockPersonalWorkspaceParentForCreate(
	ctx context.Context,
	parent *common_service_models.Organization,
	ownerID common_service_models.UserIdType,
) (*common_service_models.Organization, error) {
	tx, ok := scontext.GetTransaction(ctx)
	if !ok || tx == nil || tx.GetDB() == nil {
		return nil, errors.ErrTransactionNotFound
	}

	expectedRootID, valid := personalWorkspaceRootID(parent)
	if !valid {
		return nil, errors.ErrPersonalWorkspaceConflict
	}
	if err := h.persistenceHandler.LockPersonalWorkspaceOwner(
		ctx,
		uint64(ownerID),
		common_persistence.PersonalWorkspaceOwnerLockOptions{},
	); err != nil {
		if stdErrors.Is(err, common_persistence.ErrUserNotFound) ||
			stdErrors.Is(err, common_persistence.ErrPersonalWorkspaceConflict) {
			return nil, errors.ErrPersonalWorkspaceConflict
		}
		return nil, errors.ErrInternalError
	}
	lockedRoot, err := h.persistenceHandler.GetPersonalWorkspaceRootByOwner(
		ctx,
		uint64(ownerID),
		common_persistence.PersonalWorkspaceReadOptions{ForUpdate: true},
	)
	if err != nil {
		if stdErrors.Is(err, common_persistence.ErrOrganizationNotFound) {
			return nil, errors.ErrOrganizationParentNotFound
		}
		if stdErrors.Is(err, common_persistence.ErrPersonalWorkspaceConflict) {
			return nil, errors.ErrPersonalWorkspaceConflict
		}
		return nil, errors.ErrInternalError
	}
	lockedRootOwner, lockedRootPersonal := persistencePersonalOwnerUserID(lockedRoot)
	if lockedRoot == nil || lockedRoot.ID != int64(expectedRootID) ||
		lockedRoot.ParentOrganizationID != nil || len(lockedRoot.Lineage) != 0 ||
		!lockedRootPersonal || lockedRootOwner != ownerID {
		return nil, errors.ErrPersonalWorkspaceConflict
	}
	if parent.ID == lockedRoot.ID {
		return h.converter.PersistenceOrganizationToService(lockedRoot), nil
	}

	reloadedParent, err := h.persistenceHandler.GetOrganizationByID(ctx, parent.ID)
	if err != nil {
		if stdErrors.Is(err, common_persistence.ErrOrganizationNotFound) {
			return nil, errors.ErrOrganizationParentNotFound
		}
		return nil, errors.ErrInternalError
	}
	reloaded := h.converter.PersistenceOrganizationToService(reloadedParent)
	reloadedOwner, personal := personalOwnerUserID(reloaded)
	reloadedRootID, valid := personalWorkspaceRootID(reloaded)
	if !personal || reloadedOwner != ownerID || !valid || reloadedRootID != expectedRootID {
		return nil, errors.ErrPersonalWorkspaceConflict
	}
	return reloaded, nil
}
