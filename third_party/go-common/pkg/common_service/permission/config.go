package permission

import (
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// PermissionConfig carries state used by the permission backend flow.
type PermissionConfig struct {
	// AdminAccess is the minimum user type required for basic admin panel access
	AdminAccess proto_models.UserType

	// UserListing is the minimum user type required to list users
	UserListing proto_models.UserType

	// UserViewing is the minimum user type required to view user details
	UserViewing proto_models.UserType

	// UserBanning is the minimum user type required to ban or unban users
	UserBanning proto_models.UserType

	// UserModification is the minimum user type required to modify user accounts
	UserModification proto_models.UserType

	// UserDeletion is the minimum user type required to delete user accounts
	UserDeletion proto_models.UserType

	// UserTypeChange is the minimum user type required to change user types
	UserTypeChange proto_models.UserType

	// SystemAccess is the minimum user type required for system settings access
	SystemAccess proto_models.UserType

	// ContentModeration is the minimum user type required for moderation operations
	ContentModeration proto_models.UserType

	// AttachmentAccess is the minimum user type required for attachment admin operations
	AttachmentAccess proto_models.UserType

	// AttachmentModification is the minimum user type required for attachment modification operations
	AttachmentModification proto_models.UserType

	// AnalyticsAccess is the minimum user type required for analytics operations
	AnalyticsAccess proto_models.UserType

	// AdminCreateOrganization is the minimum user type required for admin org creation
	AdminCreateOrganization proto_models.UserType

	// AdminListOrganizations is the minimum user type required for admin org listing
	AdminListOrganizations proto_models.UserType

	// AdminManageOrgMembers is the minimum user type required for admin org member/domain management
	AdminManageOrgMembers proto_models.UserType

	// AdminGetOrganizationMember is the minimum user type required for admin org member detail access
	AdminGetOrganizationMember proto_models.UserType

	// AdminChatChannels is the minimum user type required for admin chat channel management
	AdminChatChannels proto_models.UserType
	// CareersManagement is the minimum user type required for careers management
	CareersManagement proto_models.UserType

	// ChatToolApproval is the minimum user type required to resolve agentic chat tool-call
	// approvals (Decision 6) as a site admin, alongside the triggering user / per-session channel admins.
	ChatToolApproval proto_models.UserType

	// InferenceViewing is the minimum user type required to view inference routes, rate-limit
	// policies, and attempt/usage data (AI control-plane reads)
	InferenceViewing proto_models.UserType

	// InferenceModelOverride permits request/run selection, independently of administration.
	InferenceModelOverride proto_models.UserType

	// InferenceAdministration is the minimum user type required to update inference routes and
	// model rate-limit policies (AI control-plane writes).
	InferenceAdministration proto_models.UserType

	// OrgInviteMembersUserType is the minimum user type required to invite org members
	OrgInviteMembersUserType proto_models.UserType
	// OrgInviteMembersRole is the minimum org role required to invite members
	OrgInviteMembersRole []proto_models.OrganizationRole

	// OrgAddMembersUserType is the minimum user type required to add org members
	OrgAddMembersUserType proto_models.UserType
	// OrgAddMembersRole is the minimum org role required to add members
	OrgAddMembersRole []proto_models.OrganizationRole

	// OrgRemoveMembersUserType is the minimum user type required to remove org members
	OrgRemoveMembersUserType proto_models.UserType
	// OrgRemoveMembersRole is the minimum org role required to remove members
	OrgRemoveMembersRole []proto_models.OrganizationRole

	// OrgUpdateMemberRolesUserType is the minimum user type required to update org member roles
	OrgUpdateMemberRolesUserType proto_models.UserType
	// OrgUpdateMemberRolesRole is the minimum org role required to update member roles
	OrgUpdateMemberRolesRole []proto_models.OrganizationRole

	// OrgListMembersUserType is the minimum user type required to list org members
	OrgListMembersUserType proto_models.UserType
	// OrgListMembersRole is the minimum org role required to list org members
	OrgListMembersRole []proto_models.OrganizationRole

	// OrgUpdateUserType is the minimum user type required to update organizations
	OrgUpdateUserType proto_models.UserType
	// OrgUpdateRole is the minimum org role required to update organizations
	OrgUpdateRole []proto_models.OrganizationRole

	// OrgDeleteUserType is the minimum user type required to delete organizations
	OrgDeleteUserType proto_models.UserType
	// OrgDeleteRole is the minimum org role required to delete organizations
	OrgDeleteRole []proto_models.OrganizationRole

	// OrgViewUserType is the minimum user type required to view organization details
	OrgViewUserType proto_models.UserType
	// OrgViewRole is the minimum org role required to view organization details
	OrgViewRole []proto_models.OrganizationRole

	// OrgCancelInvitationUserType is the minimum user type required to cancel org invitations
	OrgCancelInvitationUserType proto_models.UserType
	// OrgCancelInvitationRole is the minimum org role required to cancel org invitations
	OrgCancelInvitationRole []proto_models.OrganizationRole

	// OrgViewInvitationsUserType is the minimum user type required to view org invitations
	OrgViewInvitationsUserType proto_models.UserType
	// OrgViewInvitationsRole is the minimum org role required to view org invitations
	OrgViewInvitationsRole []proto_models.OrganizationRole

	// OrgManageEmailDomainsUserType is the minimum user type required to manage org email domains
	OrgManageEmailDomainsUserType proto_models.UserType
	// OrgManageEmailDomainsRole is the minimum org role required to manage org email domains
	OrgManageEmailDomainsRole []proto_models.OrganizationRole

	// OrgCreateUserType is the minimum user type required to create organizations
	OrgCreateUserType proto_models.UserType

	// OrgCreateChildUserType is the global override for the parent-role requirement.
	OrgCreateChildUserType proto_models.UserType

	// OrgCreateChildRole is the minimum org role required to create child organizations
	OrgCreateChildRole []proto_models.OrganizationRole

	// OrgSelfJoinRole is the minimum org role required for self-join into descendants
	OrgSelfJoinRole []proto_models.OrganizationRole

	OrgViewInferencePolicyRole   []proto_models.OrganizationRole
	OrgManageInferencePolicyRole []proto_models.OrganizationRole
	OrgChooseInferenceModelRole  []proto_models.OrganizationRole
}

// NewPermissionConfigFromViews builds permission thresholds from their owning consumer views.
func NewPermissionConfigFromViews(permissions config.PermissionsView, organizations config.OrganizationsView) (*PermissionConfig, error) {
	if permissions == nil || organizations == nil {
		return nil, fmt.Errorf("main config cannot be nil")
	}

	// Parse each permission threshold from string to UserType

	// Create the configuration
	cfg := &PermissionConfig{
		AdminAccess:                   permissions.SiteMinUserTypeAdminAccess(),
		UserListing:                   permissions.SiteMinUserTypeUserListing(),
		UserViewing:                   permissions.SiteMinUserTypeUserViewing(),
		UserBanning:                   permissions.SiteMinUserTypeUserBanning(),
		UserModification:              permissions.SiteMinUserTypeUserModification(),
		UserDeletion:                  permissions.SiteMinUserTypeUserDeletion(),
		UserTypeChange:                permissions.SiteMinUserTypeUserTypeChange(),
		SystemAccess:                  permissions.SiteMinUserTypeSystemAccess(),
		ContentModeration:             permissions.SiteMinUserTypeContentModeration(),
		AttachmentAccess:              permissions.SiteMinUserTypeAttachmentAccess(),
		AttachmentModification:        permissions.SiteMinUserTypeAttachmentModification(),
		AnalyticsAccess:               permissions.SiteMinUserTypeAnalyticsAccess(),
		AdminCreateOrganization:       permissions.SiteMinUserTypeAdminCreateOrganization(),
		AdminListOrganizations:        permissions.SiteMinUserTypeAdminListOrganizations(),
		AdminManageOrgMembers:         permissions.SiteMinUserTypeAdminManageOrganizationMembers(),
		AdminGetOrganizationMember:    permissions.SiteMinUserTypeAdminGetOrganizationMember(),
		AdminChatChannels:             permissions.SiteMinUserTypeAdminChatChannels(),
		CareersManagement:             permissions.SiteMinUserTypeCareersManagement(),
		ChatToolApproval:              permissions.SiteMinUserTypeChatToolApproval(),
		InferenceViewing:              permissions.SiteMinUserTypeInferenceViewing(),
		InferenceModelOverride:        safeInferenceOverrideThreshold(permissions.SiteMinUserTypeInferenceModelOverride()),
		InferenceAdministration:       permissions.SiteMinUserTypeInferenceAdministration(),
		OrgInviteMembersUserType:      organizations.OrgMinUserTypeInviteMembers(),
		OrgInviteMembersRole:          organizations.OrgMinRoleInviteMembers(),
		OrgAddMembersUserType:         organizations.OrgMinUserTypeAddMembers(),
		OrgAddMembersRole:             organizations.OrgMinRoleAddMembers(),
		OrgRemoveMembersUserType:      organizations.OrgMinUserTypeRemoveMembers(),
		OrgRemoveMembersRole:          organizations.OrgMinRoleRemoveMembers(),
		OrgUpdateMemberRolesUserType:  organizations.OrgMinUserTypeUpdateMemberRoles(),
		OrgUpdateMemberRolesRole:      organizations.OrgMinRoleUpdateMemberRoles(),
		OrgListMembersUserType:        organizations.OrgMinUserTypeListMembers(),
		OrgListMembersRole:            organizations.OrgMinRoleListMembers(),
		OrgUpdateUserType:             organizations.OrgMinUserTypeUpdate(),
		OrgUpdateRole:                 organizations.OrgMinRoleUpdate(),
		OrgDeleteUserType:             organizations.OrgMinUserTypeDelete(),
		OrgDeleteRole:                 organizations.OrgMinRoleDelete(),
		OrgViewUserType:               organizations.OrgMinUserTypeView(),
		OrgViewRole:                   organizations.OrgMinRoleView(),
		OrgCancelInvitationUserType:   organizations.OrgMinUserTypeCancelInvitation(),
		OrgCancelInvitationRole:       organizations.OrgMinRoleCancelInvitation(),
		OrgViewInvitationsUserType:    organizations.OrgMinUserTypeViewInvitations(),
		OrgViewInvitationsRole:        organizations.OrgMinRoleViewInvitations(),
		OrgManageEmailDomainsUserType: organizations.OrgMinUserTypeManageEmailDomains(),
		OrgManageEmailDomainsRole:     organizations.OrgMinRoleManageEmailDomains(),
		OrgCreateUserType:             organizations.OrgMinUserTypeCreate(),
		OrgCreateChildUserType:        organizations.OrgMinUserTypeCreateChild(),
		OrgCreateChildRole:            organizations.OrgMinRoleCreateChild(),
		OrgSelfJoinRole:               organizations.OrgMinRoleSelfJoin(),
		OrgViewInferencePolicyRole:    organizations.OrgMinRoleViewInferencePolicy(),
		OrgManageInferencePolicyRole:  organizations.OrgMinRoleManageInferencePolicy(),
		OrgChooseInferenceModelRole:   organizations.OrgMinRoleChooseInferenceModel(),
	}

	// Validate the configuration
	if err := cfg.validateRoleThresholdDepth(organizations.OrgHierarchyMaxDepth()); err != nil {
		return nil, fmt.Errorf("invalid permission configuration: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid permission configuration: %w", err)
	}

	return cfg, nil
}

// requiredUserTypeForSite executes this step in the permission backend flow.
func (c *PermissionConfig) requiredUserTypeForSite(operation SitePermissionOperation) (proto_models.UserType, bool) {
	switch operation {
	case PermissionAdminAccess:
		return c.AdminAccess, true
	case PermissionUserListing:
		return c.UserListing, true
	case PermissionUserViewing:
		return c.UserViewing, true
	case PermissionUserBanning:
		return c.UserBanning, true
	case PermissionUserModification:
		return c.UserModification, true
	case PermissionUserDeletion:
		return c.UserDeletion, true
	case PermissionUserTypeChange:
		return c.UserTypeChange, true
	case PermissionSystemAccess:
		return c.SystemAccess, true
	case PermissionContentModeration:
		return c.ContentModeration, true
	case PermissionAttachmentAccess:
		return c.AttachmentAccess, true
	case PermissionAttachmentModification:
		return c.AttachmentModification, true
	case PermissionAnalyticsAccess:
		return c.AnalyticsAccess, true
	case PermissionAdminCreateOrganization:
		return c.AdminCreateOrganization, true
	case PermissionAdminListOrganizations:
		return c.AdminListOrganizations, true
	case PermissionAdminManageOrganizationMembers:
		return c.AdminManageOrgMembers, true
	case PermissionAdminGetOrganizationMember:
		return c.AdminGetOrganizationMember, true
	case PermissionAdminChatChannels:
		return c.AdminChatChannels, true
	case PermissionCareersManagement:
		return c.CareersManagement, true
	case PermissionChatToolApproval:
		return c.ChatToolApproval, true
	case PermissionInferenceViewing:
		return c.InferenceViewing, true
	case PermissionInferenceModelOverride:
		return c.InferenceModelOverride, true
	case PermissionInferenceAdministration:
		return c.InferenceAdministration, true
	default:
		return proto_models.UserType_USER_TYPE_UNSPECIFIED, false
	}
}

// requiredUserTypeForOrg executes this step in the permission backend flow.
func (c *PermissionConfig) requiredUserTypeForOrg(operation OrganizationPermissionOperation) (proto_models.UserType, bool) {
	switch operation {
	case PermissionOrganizationInviteMembers:
		return c.OrgInviteMembersUserType, true
	case PermissionOrganizationAddMembers:
		return c.OrgAddMembersUserType, true
	case PermissionOrganizationRemoveMembers:
		return c.OrgRemoveMembersUserType, true
	case PermissionOrganizationUpdateMemberRoles:
		return c.OrgUpdateMemberRolesUserType, true
	case PermissionOrganizationListMembers:
		return c.OrgListMembersUserType, true
	case PermissionOrganizationView:
		return c.OrgViewUserType, true
	case PermissionOrganizationUpdate:
		return c.OrgUpdateUserType, true
	case PermissionOrganizationDelete:
		return c.OrgDeleteUserType, true
	case PermissionOrganizationCreate:
		return c.OrgCreateUserType, true
	case PermissionOrganizationCancelInvitation:
		return c.OrgCancelInvitationUserType, true
	case PermissionOrganizationViewInvitations:
		return c.OrgViewInvitationsUserType, true
	case PermissionOrganizationManageEmailDomains:
		return c.OrgManageEmailDomainsUserType, true
	case PermissionOrganizationCreateChild:
		return c.OrgCreateChildUserType, true
	case PermissionOrganizationSelfJoin:
		return c.AdminManageOrgMembers, true
	default:
		return proto_models.UserType_USER_TYPE_UNSPECIFIED, false
	}
}

// requiredOrgRole executes this step in the permission backend flow.
func (c *PermissionConfig) requiredOrgRole(operation OrganizationPermissionOperation, depth int) (proto_models.OrganizationRole, bool) {
	roles, exists := c.requiredOrgRoles(operation)
	if !exists || depth < 0 || depth >= len(roles) {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, false
	}
	return roles[depth], true
}

func (c *PermissionConfig) requiredOrgRoles(operation OrganizationPermissionOperation) ([]proto_models.OrganizationRole, bool) {
	switch operation {
	case PermissionOrganizationInviteMembers:
		return c.OrgInviteMembersRole, true
	case PermissionOrganizationAddMembers:
		return c.OrgAddMembersRole, true
	case PermissionOrganizationRemoveMembers:
		return c.OrgRemoveMembersRole, true
	case PermissionOrganizationUpdateMemberRoles:
		return c.OrgUpdateMemberRolesRole, true
	case PermissionOrganizationListMembers:
		return c.OrgListMembersRole, true
	case PermissionOrganizationView:
		return c.OrgViewRole, true
	case PermissionOrganizationUpdate:
		return c.OrgUpdateRole, true
	case PermissionOrganizationDelete:
		return c.OrgDeleteRole, true
	case PermissionOrganizationCancelInvitation:
		return c.OrgCancelInvitationRole, true
	case PermissionOrganizationViewInvitations:
		return c.OrgViewInvitationsRole, true
	case PermissionOrganizationManageEmailDomains:
		return c.OrgManageEmailDomainsRole, true
	case PermissionOrganizationCreateChild:
		return c.OrgCreateChildRole, true
	case PermissionOrganizationSelfJoin:
		return c.OrgSelfJoinRole, true
	case PermissionViewInferencePolicy:
		return c.OrgViewInferencePolicyRole, true
	case PermissionManageInferencePolicy:
		return c.OrgManageInferencePolicyRole, true
	case PermissionChooseInferenceModel:
		return c.OrgChooseInferenceModelRole, true
	default:
		return nil, false
	}
}

// Validate executes this step in the permission backend flow.
func (c *PermissionConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("permission config cannot be nil")
	}

	// Check that all site operations have valid thresholds
	for _, operation := range AllSiteOperations {
		threshold, exists := c.requiredUserTypeForSite(operation)
		if !exists {
			return fmt.Errorf("missing threshold for site operation: %s", operation.String())
		}
		if err := validateUserTypeThreshold(operation.String(), threshold); err != nil {
			return err
		}
	}

	// Check that all org operations have valid thresholds
	for _, operation := range AllOrgOperations {
		if !requiresOrganizationRole(operation) {
			threshold, exists := c.requiredUserTypeForOrg(operation)
			if !exists {
				return fmt.Errorf("missing threshold for org operation: %s", operation.String())
			}
			if err := validateUserTypeThreshold(operation.String(), threshold); err != nil {
				return err
			}
		}

		roles, hasRoleThreshold := c.requiredOrgRoles(operation)
		if !hasRoleThreshold {
			// Organization creation is authorized only through its global UserType
			// threshold and intentionally has no organization-role alternative.
			if operation == PermissionOrganizationCreate {
				continue
			}
			return fmt.Errorf("missing role threshold for org operation: %s", operation.String())
		}
		allowUnspecified := operation == PermissionOrganizationView
		if requiresOrganizationRole(operation) && len(roles) == 0 {
			return fmt.Errorf("missing role threshold for org operation: %s", operation.String())
		}
		for depth, role := range roles {
			if _, known := proto_models.OrganizationRole_name[int32(role)]; !known {
				return fmt.Errorf("%s role threshold at depth %d must be a known OrganizationRole, got: %d", operation.String(), depth, role)
			}
			if role == proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED && !allowUnspecified {
				return fmt.Errorf("%s role threshold at depth %d must be specified", operation.String(), depth)
			}
		}
	}

	// Validate that destructive operations require higher privileges
	// User deletion should require at least as much privilege as user modification
	if CompareUserTypes(c.UserDeletion, c.UserModification) < 0 {
		return fmt.Errorf("user deletion permission (%s) should require at least as much privilege as user modification (%s)",
			c.UserDeletion, c.UserModification)
	}
	if CompareUserTypes(c.UserTypeChange, c.UserModification) < 0 {
		return fmt.Errorf("user type change permission (%s) should require at least as much privilege as user modification (%s)",
			c.UserTypeChange, c.UserModification)
	}
	// Attachment mutations (status changes, deletion, finalizing on behalf of
	// other users) must require at least as much privilege as attachment reads.
	if CompareUserTypes(c.AttachmentModification, c.AttachmentAccess) < 0 {
		return fmt.Errorf("attachment modification permission (%s) should require at least as much privilege as attachment access (%s)",
			c.AttachmentModification, c.AttachmentAccess)
	}
	// Override thresholds are independently restricted to administrators.
	if err := config.ValidateInferenceModelOverrideThreshold(c.InferenceModelOverride); err != nil {
		return err
	}
	// Control-plane writes must require at least as much privilege as reads.
	if CompareUserTypes(c.InferenceAdministration, c.InferenceViewing) < 0 {
		return fmt.Errorf("inference administration permission (%s) should require at least as much privilege as inference viewing (%s)",
			c.InferenceAdministration, c.InferenceViewing)
	}

	return nil
}

// validateRoleThresholdDepth ensures every organization-role policy covers
// every supported hierarchy depth. PermissionConfig alone does not carry the
// hierarchy limit, so this belongs at the view-projection boundary where both
// values are available.
func (c *PermissionConfig) validateRoleThresholdDepth(maxDepth int) error {
	if maxDepth < 0 {
		return fmt.Errorf("organization hierarchy max depth must be non-negative, got: %d", maxDepth)
	}
	expected := maxDepth + 1
	for _, operation := range AllOrgOperations {
		roles, exists := c.requiredOrgRoles(operation)
		if !exists {
			if operation == PermissionOrganizationCreate {
				continue
			}
			return fmt.Errorf("missing role threshold for org operation: %s", operation.String())
		}
		if len(roles) != expected {
			return fmt.Errorf(
				"%s role threshold length must be %d for organization hierarchy depth %d, got: %d",
				operation.String(),
				expected,
				maxDepth,
				len(roles),
			)
		}
	}
	return nil
}

func validateUserTypeThreshold(name string, threshold proto_models.UserType) error {
	if _, known := proto_models.UserType_name[int32(threshold)]; !known {
		return fmt.Errorf("%s threshold must be a known UserType, got: %d", name, threshold)
	}
	if threshold == proto_models.UserType_USER_TYPE_UNSPECIFIED {
		return fmt.Errorf("%s threshold must be a specified UserType", name)
	}
	return nil
}

func safeInferenceOverrideThreshold(value proto_models.UserType) proto_models.UserType {
	if value == proto_models.UserType_USER_TYPE_UNSPECIFIED {
		return proto_models.UserType_USER_TYPE_ADMIN_TIER_2
	}
	return value
}
