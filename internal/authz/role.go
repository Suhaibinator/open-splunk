package authz

// OrgRole is a principal's effective role in the active organization. It
// mirrors go-common's OrganizationRole families, including roles inherited
// from a parent organization. The zero value grants nothing.
type OrgRole uint8

// Organization roles in ascending privilege order.
const (
	OrgRoleUnspecified OrgRole = iota
	OrgRoleMember
	OrgRoleModerator
	OrgRoleAdmin
	OrgRoleOwner
)

var orgRoleNames = [...]string{
	OrgRoleUnspecified: "unspecified",
	OrgRoleMember:      "member",
	OrgRoleModerator:   "moderator",
	OrgRoleAdmin:       "admin",
	OrgRoleOwner:       "owner",
}

// String returns the role's canonical lowercase name.
func (role OrgRole) String() string {
	if int(role) < len(orgRoleNames) {
		return orgRoleNames[role]
	}
	return orgRoleNames[OrgRoleUnspecified]
}

// Valid reports whether role is a real organization role. The unspecified
// zero value and out-of-range values are not.
func (role OrgRole) Valid() bool {
	return role >= OrgRoleMember && role <= OrgRoleOwner
}

// ParseOrgRole returns the role whose canonical name is value. It accepts
// only member, moderator, admin, and owner.
func ParseOrgRole(value string) (OrgRole, bool) {
	for role := OrgRoleMember; role <= OrgRoleOwner; role++ {
		if orgRoleNames[role] == value {
			return role, true
		}
	}
	return OrgRoleUnspecified, false
}

// Capabilities granted by each role tier on top of the tier below it.
var (
	memberGrants = []Capability{
		CapabilitySearch,
		CapabilityObjectsOwn,
	}
	moderatorGrants = []Capability{
		CapabilityKnowledgeWrite,
		CapabilityLookupsWrite,
		CapabilityAlertsWrite,
		CapabilityReportsWrite,
	}
	adminGrants = []Capability{
		CapabilityIndexesAdmin,
		CapabilityIngestionAdmin,
		CapabilityCollectorsAdmin,
		CapabilityHECAdmin,
		CapabilityAppsAdmin,
		CapabilityAuditRead,
		CapabilityKnowledgeAdmin,
		CapabilityMembersAdmin,
	}
	ownerGrants = []Capability{
		CapabilityOrgAdmin,
	}
	siteAdminGrants = []Capability{
		CapabilityServerAdmin,
		CapabilityUsersAdmin,
		CapabilityOrgsAdmin,
	}
)

// roleSets and siteAdminSet are computed once at package initialization.
// CapabilitySet is a value type, so handing them out cannot let a caller
// change the table.
var (
	roleSets     = cumulativeRoleSets()
	siteAdminSet = roleSets[OrgRoleOwner].with(siteAdminGrants...)
)

func cumulativeRoleSets() [OrgRoleOwner + 1]CapabilitySet {
	tierGrants := [...][]Capability{
		OrgRoleMember:    memberGrants,
		OrgRoleModerator: moderatorGrants,
		OrgRoleAdmin:     adminGrants,
		OrgRoleOwner:     ownerGrants,
	}
	var result [OrgRoleOwner + 1]CapabilitySet
	var cumulative CapabilitySet
	for role := OrgRoleMember; role <= OrgRoleOwner; role++ {
		cumulative = cumulative.with(tierGrants[role]...)
		result[role] = cumulative
	}
	return result
}

// CapabilitiesFor returns the capabilities of a principal holding role in the
// active organization. A site administrator holds every organization
// capability in any organization, whether or not it is a member, plus the
// site-only capabilities. An invalid role without site administration grants
// nothing.
func CapabilitiesFor(role OrgRole, siteAdmin bool) CapabilitySet {
	if siteAdmin {
		return siteAdminSet
	}
	if !role.Valid() {
		return CapabilitySet{}
	}
	return roleSets[role]
}

// go-common's OrganizationRole and UserType enums both number their values in
// four bands of eleven tiers starting at 1 (MEMBER_TIER_0 = 1 … OWNER_TIER_10
// = 44; LITE_TIER_0 = 1 … ADMIN_TIER_10 = 44), with 0 as UNSPECIFIED.
const (
	goCommonBandOrigin = 1
	goCommonBandWidth  = 11
	goCommonBands      = len(goCommonOrgRoleBands)
	// goCommonSiteAdminBand is the UserType Admin band (USER_TYPE_ADMIN_TIER_*).
	goCommonSiteAdminBand = 3
)

// goCommonOrgRoleBands is the organization role of each OrganizationRole
// band, lowest first.
var goCommonOrgRoleBands = [...]OrgRole{OrgRoleMember, OrgRoleModerator, OrgRoleAdmin, OrgRoleOwner}

// goCommonBand returns the zero-based band of a go-common tiered enum value,
// or false for UNSPECIFIED, negative, and out-of-range values.
func goCommonBand(value int32) (int, bool) {
	if value < goCommonBandOrigin {
		return 0, false
	}
	band := int(value-goCommonBandOrigin) / goCommonBandWidth
	if band >= goCommonBands {
		return 0, false
	}
	return band, true
}

// OrgRoleFromGoCommon maps a go-common OrganizationRole enum value, at any
// tier, to its organization role. UNSPECIFIED and values outside the four
// bands map to OrgRoleUnspecified, which grants nothing. Callers must use this
// instead of converting the enum number directly: OrgRole(2) is moderator,
// while go-common value 2 is MEMBER_TIER_1.
func OrgRoleFromGoCommon(value int32) OrgRole {
	band, ok := goCommonBand(value)
	if !ok {
		return OrgRoleUnspecified
	}
	return goCommonOrgRoleBands[band]
}

// SiteAdminFromGoCommonUserType reports whether a go-common UserType enum
// value is in the Admin band, at any tier. UNSPECIFIED, the lower bands, and
// unknown values are not site administrators.
func SiteAdminFromGoCommonUserType(value int32) bool {
	band, ok := goCommonBand(value)
	return ok && band == goCommonSiteAdminBand
}
