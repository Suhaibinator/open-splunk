package identity

import (
	"time"

	gcconfig "github.com/Suhaibinator/go-common/pkg/config"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

const (
	TeamOrganizationType  = pb.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0
	PersonalWorkspaceType = pb.OrganizationType_ORGANIZATION_TYPE_BASIC_TIER_0
)

// Configure applies OpenSplunk's identity policy without changing deployment
// resources (PostgreSQL, MinIO, cookie security, signing keys and origins).
func Configure(c *gcconfig.Config) {
	c.PathPrefix = new(Prefix)
	c.EnableOAuth = new(true)
	c.EnableSessionRefresh = new(true)
	c.RateLimitEnabled = new(true)
	c.EnableEmailAuth = new(false)
	c.SMSEnabled = new(false)
	c.EmailEnabled = new(false)
	c.EnableOrganizations = new(true)
	// Keep GoCommon's managed workspace lifecycle; only team organizations
	// appear in the OpenSplunk catalog. Basic workspaces cannot create children.
	c.PersonalWorkspaceOrganizationType = new(PersonalWorkspaceType)
	c.EnableAdminAPI = new(true)
	c.EnableUserSettings = new(false)
	c.EnableUserProfiles = new(false)
	c.EnableUserSearch = new(false)
	c.EnableAttachments = new(false)
	c.EnableContacts = new(false)
	c.EnableCareers = new(false)
	c.EnableFaro = new(false)
	c.EnableChat = new(false)
	c.EnableRealtimeWebSocket = new(false)
	c.EnableNotifications = new(false)
	c.EnableOrganizationEmailDomains = new(false)
	c.EnableOrganizationEmailAutoAssociation = new(false)
	c.AllowOrganizationManagedEmailDomains = new(false)
	c.OrgAncestorMembershipAutoAdd = new(false)
	// Keep the native token cache lifetime positive. Mutable user projections
	// must be reloaded for manual database promotions and native /user/me.
	c.AuthCacheTtl = gcconfig.GetDefaults().AuthCacheTtl
	c.AuthUserCacheCapacity = new(uint64(0))
	c.OrganizationRoleCacheTTL = new(time.Duration(0))
	c.OAuthSignupMode = new(gcconfig.OAuthSignupModeEmailLocalPart)
	c.DefaultUserType = new(pb.UserType_USER_TYPE_REGULAR_TIER_0)
	c.RequireRealNamesOnSignup = new(false)
	c.RequireUsernamesOnSignup = new(false)
	site := pb.UserType_USER_TYPE_ADMIN_TIER_10
	c.SiteMinUserTypeAdminAccess = new(site)
	c.SiteMinUserTypeUserListing = new(site)
	c.SiteMinUserTypeUserViewing = new(site)
	c.SiteMinUserTypeUserBanning = new(site)
	c.SiteMinUserTypeUserModification = new(site)
	c.SiteMinUserTypeUserDeletion = new(site)
	c.SiteMinUserTypeUserTypeChange = new(site)
	c.SiteMinUserTypeSystemAccess = new(site)
	c.SiteMinUserTypeContentModeration = new(site)
	c.SiteMinUserTypeAttachmentAccess = new(site)
	c.SiteMinUserTypeAttachmentModification = new(site)
	c.SiteMinUserTypeAnalyticsAccess = new(site)
	c.SiteMinUserTypeAdminCreateOrganization = new(site)
	c.SiteMinUserTypeAdminListOrganizations = new(site)
	c.SiteMinUserTypeAdminManageOrganizationMembers = new(site)
	c.SiteMinUserTypeAdminGetOrganizationMember = new(site)
	c.SiteMinUserTypeAdminChatChannels = new(site)
	c.SiteMinUserTypeCareersManagement = new(site)
	c.SiteMinUserTypeChatToolApproval = new(site)
	c.SiteMinUserTypeInferenceViewing = new(site)
	c.SiteMinUserTypeInferenceModelOverride = new(site)
	c.SiteMinUserTypeInferenceAdministration = new(site)
	c.OrgMinUserTypeManageEmailDomains = new(site)
	// Native creation is exposed only by the application's explicit-owner
	// route. Allow its actual regular parent Admin actor through native child
	// creation; that application route independently restricts root creation.
	c.OrgMinUserTypeCreate = new(pb.UserType_USER_TYPE_REGULAR_TIER_0)
	c.OrgMinUserTypeCreateChild = new(site)
	c.OrgMinUserTypeView = new(site)
	c.OrgMinUserTypeListMembers = new(site)
	c.OrgMinUserTypeDelete = new(site)
	c.OrgMinUserTypeUpdate = new(site)
	c.OrgMinUserTypeInviteMembers = new(site)
	c.OrgMinUserTypeAddMembers = new(site)
	c.OrgMinUserTypeRemoveMembers = new(site)
	c.OrgMinUserTypeUpdateMemberRoles = new(site)
	c.OrgMinUserTypeCancelInvitation = new(site)
	c.OrgMinUserTypeViewInvitations = new(site)
	viewer := pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0
	admin := pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0
	owner := pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10
	c.OrgMinRoleView = roleDepths(c, viewer)
	c.OrgMinRoleListMembers = roleDepths(c, viewer)
	c.OrgMinRoleUpdate = roleDepths(c, admin)
	c.OrgMinRoleAddMembers = roleDepths(c, admin)
	c.OrgMinRoleRemoveMembers = roleDepths(c, admin)
	c.OrgMinRoleUpdateMemberRoles = roleDepths(c, admin)
	c.OrgMinRoleInviteMembers = roleDepths(c, admin)
	c.OrgMinRoleCancelInvitation = roleDepths(c, admin)
	c.OrgMinRoleViewInvitations = roleDepths(c, admin)
	c.OrgMinRoleCreateChild = roleDepths(c, admin)
	c.OrgMinRoleDelete = roleDepths(c, owner)
	c.DefaultOrgCreatorRole = roleDepths(c, admin)
	c.OrgAutoAddCreatorOnChildCreate = make([]bool, len(c.DefaultOrgCreatorRole))
}

func roleDepths(c *gcconfig.Config, role pb.OrganizationRole) []pb.OrganizationRole {
	depth := max(0, gcconfig.NewRootView(c).OrgHierarchyMaxDepth())
	roles := make([]pb.OrganizationRole, depth+1)
	for i := range roles {
		roles[i] = role
	}
	return roles
}
