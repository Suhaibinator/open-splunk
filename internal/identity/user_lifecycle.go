package identity

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	gcpersistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	gerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// Native account deletion soft-deletes the core user. Remove its memberships
// through GoCommon's guarded hierarchy primitive first, in the same transaction,
// so deleting an account cannot strand the last Owner or leave child memberships.
func (r *Runtime) removeDeletedUserMemberships(ctx context.Context, userID sm.UserIdType) error {
	tx, ok := scontext.GetTransaction(ctx)
	if !ok || tx == nil || tx.GetDB() == nil {
		return gerrors.ErrTransactionNotFound
	}
	memberships, _, err := r.persistence.GetUserOrganizations(ctx, userID, nil)
	if err != nil {
		return err
	}
	roots := make([]int64, 0, len(memberships))
	for _, membership := range memberships {
		if membership == nil || membership.UserOrganization == nil || membership.UserOrganization.Organization.ID <= 0 {
			return fmt.Errorf("identity: invalid organization membership during account deletion")
		}
		// GoCommon archives the managed workspace after this callback.
		if membership.UserOrganization.Organization.PersonalOwnerUserID != nil {
			continue
		}
		if membership.UserOrganization.Organization.ParentOrganizationID == nil {
			roots = append(roots, membership.UserOrganization.OrganizationID)
		}
	}
	slices.Sort(roots)
	for _, root := range roots {
		err := r.persistence.RemoveUserFromOrganizationAndDescendantsGuardingLastOwner(ctx, userID, root, pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10)
		if errors.Is(err, gcpersistence.ErrOrganizationLastOwner) {
			return gerrors.ErrCannotRemoveLastOwner
		}
		if err != nil {
			return err
		}
	}
	remaining, _, err := r.persistence.GetUserOrganizations(ctx, userID, nil)
	if err != nil {
		return err
	}
	for _, membership := range remaining {
		if membership == nil || membership.UserOrganization == nil || membership.UserOrganization.Organization.PersonalOwnerUserID == nil {
			return fmt.Errorf("identity: team organization membership remained after guarded account deletion")
		}
	}
	return nil
}
