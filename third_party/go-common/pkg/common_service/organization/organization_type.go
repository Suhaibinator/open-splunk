package organization

import (
	"context"
	stdErrors "errors"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

type organizationTypeCapability int

const (
	organizationTypeCapabilityUnknown organizationTypeCapability = iota
	organizationTypeCapabilityListMembers
	organizationTypeCapabilityInviteMembers
	organizationTypeCapabilityAddMembers
	organizationTypeCapabilityRemoveMembers
	organizationTypeCapabilityUpdateMemberRoles
	organizationTypeCapabilityViewInvitations
	organizationTypeCapabilityCancelInvitation
	organizationTypeCapabilityManageEmailDomains
	organizationTypeCapabilityWorkspaceNavigation
	organizationTypeCapabilityCreateChildOrgs
	organizationTypeCapabilityBulkMemberManagement

	organizationTypeCapabilityCount
)

var organizationTypeCapabilityNames = [...]string{
	organizationTypeCapabilityUnknown:              "unknown",
	organizationTypeCapabilityListMembers:          "list_members",
	organizationTypeCapabilityInviteMembers:        "invite_members",
	organizationTypeCapabilityAddMembers:           "add_members",
	organizationTypeCapabilityRemoveMembers:        "remove_members",
	organizationTypeCapabilityUpdateMemberRoles:    "update_member_roles",
	organizationTypeCapabilityViewInvitations:      "view_invitations",
	organizationTypeCapabilityCancelInvitation:     "cancel_invitation",
	organizationTypeCapabilityManageEmailDomains:   "manage_email_domains",
	organizationTypeCapabilityWorkspaceNavigation:  "workspace_navigation",
	organizationTypeCapabilityCreateChildOrgs:      "create_child_organizations",
	organizationTypeCapabilityBulkMemberManagement: "bulk_member_management",
}

// String returns the stable text key for an organizationTypeCapability value and is used in logs and diagnostics around capability enforcement.
//
// Returns:
//   - string: Capability name, or `"unknown"` for out-of-range values.
func (c organizationTypeCapability) String() string {
	if c < 0 || c >= organizationTypeCapabilityCount {
		return organizationTypeCapabilityNames[organizationTypeCapabilityUnknown]
	}
	return organizationTypeCapabilityNames[c]
}

// resolvedOrganization wraps a resolved service organization used to avoid repeating persistence lookups across capability and membership helpers.
type resolvedOrganization struct {
	Organization *common_service_models.Organization
}

// getRequiredOrganizationType maps a capability to the minimum organization type configured to allow that capability.
//
// Parameters:
//   - capability: Capability being enforced for an organization operation.
//
// Returns:
//   - proto_models.OrganizationType: Minimum type that satisfies the capability.
func (h *OrganizationHandler[U]) getRequiredOrganizationType(capability organizationTypeCapability) proto_models.OrganizationType {
	switch capability {
	case organizationTypeCapabilityListMembers:
		return h.runtimeConfig().OrgMinTypeListMembers()
	case organizationTypeCapabilityInviteMembers:
		return h.runtimeConfig().OrgMinTypeInviteMembers()
	case organizationTypeCapabilityAddMembers:
		return h.runtimeConfig().OrgMinTypeAddMembers()
	case organizationTypeCapabilityRemoveMembers:
		return h.runtimeConfig().OrgMinTypeRemoveMembers()
	case organizationTypeCapabilityUpdateMemberRoles:
		return h.runtimeConfig().OrgMinTypeUpdateMemberRoles()
	case organizationTypeCapabilityViewInvitations:
		return h.runtimeConfig().OrgMinTypeViewInvitations()
	case organizationTypeCapabilityCancelInvitation:
		return h.runtimeConfig().OrgMinTypeCancelInvitation()
	case organizationTypeCapabilityManageEmailDomains:
		return h.runtimeConfig().OrgMinTypeManageEmailDomains()
	case organizationTypeCapabilityWorkspaceNavigation:
		return h.runtimeConfig().OrgMinTypeWorkspaceNavigation()
	case organizationTypeCapabilityCreateChildOrgs:
		return h.runtimeConfig().OrgMinTypeCreateChildOrganizations()
	case organizationTypeCapabilityBulkMemberManagement:
		return h.runtimeConfig().OrgMinTypeBulkMemberManagement()
	default:
		return h.runtimeConfig().DefaultOrganizationType()
	}
}

// resolveOrganization loads an organization, converts it to the service model, and seeds depth cache used by downstream authorization checks.
//
// Parameters:
//   - ctx: Request context used for persistence access.
//   - orgID: Organization ID to resolve.
//
// Returns:
//   - *resolvedOrganization: Wrapper containing the resolved service organization.
//   - error: Invalid-ID, not-found, or internal lookup error.
func (h *OrganizationHandler[U]) resolveOrganization(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
) (*resolvedOrganization, error) {
	if orgID == 0 {
		return nil, errors.ErrInvalidOrganizationID
	}

	inTransaction := hasAmbientTransaction(ctx)
	cacheable := h.orgDepthCache != nil && !inTransaction
	var generation uint64
	if cacheable {
		generation = h.orgDepthCache.Generation()
	}

	org, err := h.persistenceHandler.GetOrganizationByID(ctx, orgID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return nil, errors.ErrOrganizationNotFound
		}
		return nil, errors.ErrInternalError
	}

	serviceOrg := h.converter.PersistenceOrganizationToService(org)
	if cacheable {
		h.orgDepthCache.FillIfCurrent(orgID, len(serviceOrg.GetLineage()), generation)
	}

	return &resolvedOrganization{
		Organization: serviceOrg,
	}, nil
}

// organizationDepth returns the lineage-based depth of an organization model and is used for depth-aware permission checks.
//
// Parameters:
//   - org: Organization model providing lineage data.
//
// Returns:
//   - int: Number of lineage elements, or `0` when organization is nil.
func organizationDepth(org common_service_models.OrganizationLike) int {
	if org == nil {
		return 0
	}
	return len(org.GetLineage())
}

// ParentOrganizationID returns the parent organization ID from a resolvedOrganization wrapper.
//
// Returns:
//   - common_service_models.OrganizationIdType: Parent ID, or `0` when no parent is present.
func (r *resolvedOrganization) ParentOrganizationID() common_service_models.OrganizationIdType {
	if r == nil || r.Organization == nil || r.Organization.ParentOrganizationID == nil {
		return 0
	}
	return *r.Organization.ParentOrganizationID
}

// ensureOrganizationTypeCapabilityForOrganization validates that a resolved organization model satisfies a capability's minimum organization type.
//
// Parameters:
//   - organization: Organization model whose type is validated.
//   - capability: Capability requirement to enforce.
//
// Returns:
//   - error: Not-found or capability-denied error when requirements are not met.
func (h *OrganizationHandler[U]) ensureOrganizationTypeCapabilityForOrganization(
	organization common_service_models.OrganizationLike,
	capability organizationTypeCapability,
) error {
	if organization == nil {
		return errors.ErrOrganizationNotFound
	}
	return h.ensureOrganizationTypeCapabilityForValue(organization.GetOrganizationType(), capability)
}

// ensureOrganizationTypeCapability resolves an organization by ID and enforces capability requirements against its configured organization type.
//
// Parameters:
//   - ctx: Request context used for organization resolution.
//   - orgID: Organization ID whose capability is being checked.
//   - capability: Capability requirement to enforce.
//
// Returns:
//   - error: Invalid-ID, not-found, or capability-denied error.
func (h *OrganizationHandler[U]) ensureOrganizationTypeCapability(
	ctx context.Context,
	orgID common_service_models.OrganizationIdType,
	capability organizationTypeCapability,
) error {
	if orgID == 0 {
		return errors.ErrInvalidOrganizationID
	}

	resolvedOrg, err := h.resolveOrganization(ctx, orgID)
	if err != nil {
		return err
	}

	return h.ensureOrganizationTypeCapabilityForOrganization(resolvedOrg.Organization, capability)
}

// ensureOrganizationTypeCapabilityForValue enforces a capability against a raw organization type value and is shared by ID-based and model-based checks.
//
// Parameters:
//   - organizationType: Organization type value to validate.
//   - capability: Capability requirement to enforce.
//
// Returns:
//   - error: Capability-denied error when the organization type is below the required threshold.
func (h *OrganizationHandler[U]) ensureOrganizationTypeCapabilityForValue(
	organizationType proto_models.OrganizationType,
	capability organizationTypeCapability,
) error {
	requiredType := h.getRequiredOrganizationType(capability)
	if (organizationType) < requiredType {
		return errors.ErrOrganizationTypeCapabilityDenied
	}

	return nil
}
