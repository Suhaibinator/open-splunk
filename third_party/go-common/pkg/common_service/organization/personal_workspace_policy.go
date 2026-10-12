package organization

import (
	"regexp"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

const personalWorkspaceSlugNamespace = "personal"

// reservedPersonalWorkspaceSlugPattern matches exactly the minted
// "personal-<ownerID>" namespace after canonicalization.
var reservedPersonalWorkspaceSlugPattern = regexp.MustCompile(`^` + personalWorkspaceSlugNamespace + `-[0-9]+$`)

// isReservedPersonalWorkspaceSlug reports whether an ordinary organization
// request is attempting to use a slug the system mints for personal workspace
// roots ("personal-<ownerID>"). Reservation is unconditional for that exact
// namespace: it belongs to managed provisioning. Slugs that merely start with
// "personal" (for example "personalized-gifts") remain available.
func (h *OrganizationHandler[U]) isReservedPersonalWorkspaceSlug(slug string) bool {
	normalized := strings.ToLower(strings.TrimSpace(slug))
	return reservedPersonalWorkspaceSlugPattern.MatchString(normalized)
}

// personalOwnerUserID returns authoritative ownership by field presence. Slug
// and organization type are intentionally never consulted for classification.
func personalOwnerUserID(org common_service_models.OrganizationLike) (common_service_models.UserIdType, bool) {
	if org == nil || org.GetPersonalOwnerUserID() == nil {
		return 0, false
	}
	return *org.GetPersonalOwnerUserID(), true
}

func personalWorkspaceRootID(org *common_service_models.Organization) (common_service_models.OrganizationIdType, bool) {
	if org == nil || org.ID == 0 {
		return 0, false
	}
	if org.ParentOrganizationID == nil {
		return org.ID, len(org.Lineage) == 0
	}
	if *org.ParentOrganizationID == 0 || len(org.Lineage) == 0 {
		return 0, false
	}
	return org.Lineage[0], org.Lineage[0] != 0
}

func persistencePersonalOwnerUserID(org *common_persistence_models.Organization) (common_service_models.UserIdType, bool) {
	if org == nil || org.PersonalOwnerUserID == nil {
		return 0, false
	}
	return common_service_models.UserIdType(*org.PersonalOwnerUserID), true
}

func (h *OrganizationHandler[U]) ensurePersonalMembershipAddAllowed(
	org common_service_models.OrganizationLike,
	targetUserID common_service_models.UserIdType,
	role proto_models.OrganizationRole,
) error {
	ownerID, personal := personalOwnerUserID(org)
	if !personal {
		return nil
	}
	if targetUserID != ownerID || role != h.config.PersonalWorkspaceOwnerRole {
		return errors.ErrPersonalWorkspaceMembershipRestricted
	}
	return nil
}

func (h *OrganizationHandler[U]) ensurePersonalMemberRemovalAllowed(
	org common_service_models.OrganizationLike,
	targetUserID common_service_models.UserIdType,
) error {
	ownerID, personal := personalOwnerUserID(org)
	if personal && targetUserID == ownerID {
		return errors.ErrPersonalWorkspaceOwnerMembershipProtected
	}
	return nil
}

func (h *OrganizationHandler[U]) ensurePersonalMemberRoleUpdateAllowed(
	org common_service_models.OrganizationLike,
	targetUserID common_service_models.UserIdType,
	role proto_models.OrganizationRole,
) error {
	ownerID, personal := personalOwnerUserID(org)
	if !personal {
		return nil
	}
	if targetUserID != ownerID {
		return errors.ErrPersonalWorkspaceMembershipRestricted
	}
	if role != h.config.PersonalWorkspaceOwnerRole {
		return errors.ErrPersonalWorkspaceOwnerMembershipProtected
	}
	return nil
}

// ensurePersonalInvitationAllowed rejects both acceptance-required and auto-add
// invitations for personal trees. Owner membership repair is deliberately kept
// on the personal-workspace service path instead of invitation APIs.
func ensurePersonalInvitationAllowed(org common_service_models.OrganizationLike) error {
	if common_service_models.IsPersonalOrganization(org) {
		return errors.ErrPersonalWorkspaceMembershipRestricted
	}
	return nil
}

func persistenceOrganizationDescription(org *common_persistence_models.Organization) string {
	if org == nil || org.Description == nil {
		return ""
	}
	return *org.Description
}
