package permission

import (
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// Re-export proto types for use in this package and consumers.
// Using type aliases maintains full compatibility with the proto types.
type SitePermissionOperation = proto_models.SitePermissionOperation
type OrganizationPermissionOperation = proto_models.OrganizationPermissionOperation

// Site permission operations use the shared protobuf enum values.
const (
	// SitePermissionOperationUnknown represents an unspecified or invalid site operation.
	SitePermissionOperationUnknown = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_UNSPECIFIED

	// PermissionAdminAccess represents basic admin panel access.
	PermissionAdminAccess = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_ADMIN_ACCESS

	// PermissionUserListing represents the ability to list users via admin APIs.
	PermissionUserListing = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_USER_LISTING

	// PermissionUserViewing represents the ability to view user details via admin APIs.
	PermissionUserViewing = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_USER_VIEWING

	// PermissionUserBanning represents the ability to ban or unban users.
	PermissionUserBanning = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_USER_BANNING

	// PermissionUserModification represents the ability to modify user accounts.
	PermissionUserModification = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_USER_MODIFICATION

	// PermissionUserDeletion represents the ability to delete user accounts.
	PermissionUserDeletion = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_USER_DELETION

	// PermissionUserTypeChange represents the ability to change user types.
	PermissionUserTypeChange = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_USER_TYPE_CHANGE

	// PermissionSystemAccess represents access to system settings and tokens.
	PermissionSystemAccess = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_SYSTEM_ACCESS

	// PermissionContentModeration represents access to moderation tools.
	PermissionContentModeration = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_CONTENT_MODERATION

	// PermissionAttachmentAccess represents access to attachment admin tools.
	PermissionAttachmentAccess = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_ATTACHMENT_ACCESS

	// PermissionAttachmentModification represents modifying attachments.
	PermissionAttachmentModification = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_ATTACHMENT_MODIFICATION

	// PermissionAnalyticsAccess represents access to analytics endpoints.
	PermissionAnalyticsAccess = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_ANALYTICS_ACCESS

	// PermissionAdminCreateOrganization represents admin-only organization creation.
	PermissionAdminCreateOrganization = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_ADMIN_CREATE_ORGANIZATION

	// PermissionAdminListOrganizations represents admin-only organization listing.
	PermissionAdminListOrganizations = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_ADMIN_LIST_ORGANIZATIONS

	// PermissionAdminManageOrganizationMembers represents admin-level org member and domain management.
	PermissionAdminManageOrganizationMembers = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_ADMIN_MANAGE_ORGANIZATION_MEMBERS

	// PermissionAdminGetOrganizationMember represents admin-only org member detail access.
	PermissionAdminGetOrganizationMember = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_ADMIN_GET_ORGANIZATION_MEMBER

	// PermissionAdminChatChannels represents admin-only chat channel management.
	PermissionAdminChatChannels = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_ADMIN_CHAT_CHANNELS

	// PermissionCareersManagement represents admin-only careers management.
	PermissionCareersManagement = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_CAREERS_MANAGEMENT

	// PermissionChatToolApproval represents resolving agentic chat tool-call approvals (Decision 6).
	// It is the configurable site-admin authority that, alongside the triggering user and the
	// per-session channel-admin policy, may approve/deny a pending TOOL_CALL.
	PermissionChatToolApproval = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_CHAT_TOOL_APPROVAL

	// PermissionInferenceViewing represents read access to the AI control plane: operation routes,
	// model rate-limit policies, and the attempt/usage ledger.
	PermissionInferenceViewing = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_INFERENCE_VIEWING

	// PermissionInferenceModelOverride permits a request/run model choice without route administration.
	PermissionInferenceModelOverride = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_INFERENCE_MODEL_OVERRIDE

	// PermissionInferenceAdministration represents write access to the AI control plane: updating
	// operation routes and model rate-limit policies. Separate from viewing so a write freeze can
	// coexist with read access.
	PermissionInferenceAdministration = proto_models.SitePermissionOperation_SITE_PERMISSION_OPERATION_INFERENCE_ADMINISTRATION
)

// Organization permission operations use the shared protobuf enum values.
const (
	// OrgPermissionOperationUnknown represents an unspecified or invalid org operation.
	OrgPermissionOperationUnknown = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_UNSPECIFIED

	// PermissionOrganizationInviteMembers represents inviting members to an organization.
	PermissionOrganizationInviteMembers = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_INVITE_MEMBERS

	// PermissionOrganizationAddMembers represents adding members directly to an organization.
	PermissionOrganizationAddMembers = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_ADD_MEMBERS

	// PermissionOrganizationRemoveMembers represents removing members from an organization.
	PermissionOrganizationRemoveMembers = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_REMOVE_MEMBERS

	// PermissionOrganizationUpdateMemberRoles represents updating organization member roles.
	PermissionOrganizationUpdateMemberRoles = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_UPDATE_MEMBER_ROLES

	// PermissionOrganizationListMembers represents listing members within an organization.
	PermissionOrganizationListMembers = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_LIST_MEMBERS

	// PermissionOrganizationView represents viewing organization details.
	PermissionOrganizationView = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_VIEW

	// PermissionOrganizationUpdate represents updating organization details.
	PermissionOrganizationUpdate = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_UPDATE

	// PermissionOrganizationDelete represents deleting an organization.
	PermissionOrganizationDelete = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_DELETE

	// PermissionOrganizationCreate represents creating new organizations.
	PermissionOrganizationCreate = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_CREATE

	// PermissionOrganizationCancelInvitation represents canceling org invitations.
	PermissionOrganizationCancelInvitation = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_CANCEL_INVITATION

	// PermissionOrganizationViewInvitations represents viewing org invitations.
	PermissionOrganizationViewInvitations = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_VIEW_INVITATIONS

	// PermissionOrganizationManageEmailDomains represents managing org email domains.
	PermissionOrganizationManageEmailDomains = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_MANAGE_EMAIL_DOMAINS

	// PermissionOrganizationCreateChild represents creating child organizations.
	PermissionOrganizationCreateChild = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_CREATE_CHILD

	// PermissionOrganizationSelfJoin represents self-joining an organization.
	PermissionOrganizationSelfJoin = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_SELF_JOIN

	PermissionViewInferencePolicy   = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_VIEW_INFERENCE_POLICY
	PermissionManageInferencePolicy = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_MANAGE_INFERENCE_POLICY
	PermissionChooseInferenceModel  = proto_models.OrganizationPermissionOperation_ORGANIZATION_PERMISSION_OPERATION_CHOOSE_INFERENCE_MODEL
)

// AllSiteOperations contains all valid site permission operations.
var AllSiteOperations = []SitePermissionOperation{
	PermissionAdminAccess,
	PermissionUserListing,
	PermissionUserViewing,
	PermissionUserBanning,
	PermissionUserModification,
	PermissionUserDeletion,
	PermissionUserTypeChange,
	PermissionSystemAccess,
	PermissionContentModeration,
	PermissionAttachmentAccess,
	PermissionAttachmentModification,
	PermissionAnalyticsAccess,
	PermissionAdminCreateOrganization,
	PermissionAdminListOrganizations,
	PermissionAdminManageOrganizationMembers,
	PermissionAdminGetOrganizationMember,
	PermissionAdminChatChannels,
	PermissionCareersManagement,
	PermissionChatToolApproval,
	PermissionInferenceViewing,
	PermissionInferenceModelOverride,
	PermissionInferenceAdministration,
}

// AllOrgOperations contains all valid organization permission operations.
var AllOrgOperations = []OrganizationPermissionOperation{
	PermissionOrganizationInviteMembers,
	PermissionOrganizationAddMembers,
	PermissionOrganizationRemoveMembers,
	PermissionOrganizationUpdateMemberRoles,
	PermissionOrganizationListMembers,
	PermissionOrganizationView,
	PermissionOrganizationUpdate,
	PermissionOrganizationDelete,
	PermissionOrganizationCreate,
	PermissionOrganizationCancelInvitation,
	PermissionOrganizationViewInvitations,
	PermissionOrganizationManageEmailDomains,
	PermissionOrganizationCreateChild,
	PermissionOrganizationSelfJoin,
	PermissionViewInferencePolicy,
	PermissionManageInferencePolicy,
	PermissionChooseInferenceModel,
}

func requiresOrganizationRole(operation OrganizationPermissionOperation) bool {
	switch operation {
	case PermissionViewInferencePolicy, PermissionManageInferencePolicy, PermissionChooseInferenceModel:
		return true
	default:
		return false
	}
}
