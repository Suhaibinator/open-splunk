package common_service_models

import (
	"time"

	proto_models "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// Maximum length constants for organization fields validation.
const (
	// MaxOrganizationNameLength is the maximum allowed length for organization names.
	MaxOrganizationNameLength = 100

	// MaxOrganizationSlugLength is the maximum allowed length for organization slugs.
	MaxOrganizationSlugLength = 50

	// MaxOrganizationDescriptionLength is the maximum allowed length for organization descriptions.
	MaxOrganizationDescriptionLength = 500
)

// OrganizationIdType defines the type used for organization IDs throughout the service layer.
// Using a type alias allows for easy changes to the underlying type if needed.
type OrganizationIdType = int64

// Organization defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type Organization struct {
	// ID is the unique identifier for the organization.
	ID OrganizationIdType `json:"id"`

	// ParentOrganizationID references the parent organization (nil for root organizations).
	ParentOrganizationID *OrganizationIdType `json:"parent_organization_id,omitempty"`

	// Lineage stores the ordered list of ancestor org IDs (root -> parent).
	Lineage []OrganizationIdType `json:"lineage,omitempty"`

	// Name is the human-readable organization name.
	Name string `json:"name"`

	// Slug is the URL-friendly organization identifier.
	Slug string `json:"slug"`

	// Description provides additional information about the organization.
	Description string `json:"description"`

	// AvatarUrl contains the object path to the organization's avatar/logo.
	// This is the FinalObjectName from the attachment, not a full URL.
	AvatarUrl string `json:"avatar_url"`

	// OrganizationType controls organization-level capability thresholds.
	OrganizationType proto_models.OrganizationType `json:"organization_type"`

	// PersonalOwnerUserID identifies the user whose personal workspace contains this organization.
	// A nil value identifies a shared organization.
	PersonalOwnerUserID *UserIdType `json:"personal_owner_user_id,omitempty"`

	// CreatedAt indicates when the organization was created.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt indicates when the organization was last updated.
	UpdatedAt time.Time `json:"updated_at"`
}

// OrganizationHierarchyNode defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type OrganizationHierarchyNode struct {
	Organization *Organization                `json:"organization,omitempty"`
	Children     []*OrganizationHierarchyNode `json:"children,omitempty"`
}

// UserOrganization defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type UserOrganization struct {
	// OrganizationID is the ID of the organization.
	OrganizationID OrganizationIdType `json:"organization_id"`

	// UserID is the ID of the user.
	UserID UserIdType `json:"user_id"`

	// ExplicitRole is the user's direct membership role in this organization.
	ExplicitRole proto_models.OrganizationRole `json:"explicit_role"`

	// EffectiveRole is the role after applying lineage inheritance rules.
	EffectiveRole proto_models.OrganizationRole `json:"effective_role"`

	// InheritedFromOrganizationID references the ancestor organization that granted the effective role.
	// Nil when the role is explicit or no inheritance applies.
	InheritedFromOrganizationID *OrganizationIdType `json:"inherited_from_organization_id,omitempty"`

	// EffectiveRoleDepth is the absolute depth of the organization that supplied the effective role (0=root).
	// Nil when EffectiveRole is unspecified.
	EffectiveRoleDepth *uint32 `json:"effective_role_depth,omitempty"`

	// CreatedAt indicates when the user joined the organization.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt indicates when the user's role was last modified.
	UpdatedAt time.Time `json:"updated_at"`
}

// OrganizationEmailDomain defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type OrganizationEmailDomain struct {
	ID              uint64             `json:"id"`
	OrganizationID  OrganizationIdType `json:"organization_id"`
	Domain          string             `json:"domain"`
	CreatedByUserID *UserIdType        `json:"created_by_user_id,omitempty"`
	CreatedAt       time.Time          `json:"created_at"`
	UpdatedAt       time.Time          `json:"updated_at"`
}

// OrganizationEmailDomainLike defines the interface for organization email domain objects.
type OrganizationEmailDomainLike interface {
	GetID() uint64
	GetOrganizationID() OrganizationIdType
	GetDomain() string
	GetCreatedByUserID() *UserIdType
	GetCreatedAt() time.Time
	GetUpdatedAt() time.Time
}

var _ OrganizationEmailDomainLike = &OrganizationEmailDomain{}

// GetID returns the requested value for *OrganizationEmailDomain in the common_service_models backend flow.
func (d *OrganizationEmailDomain) GetID() uint64 {
	return d.ID
}

// GetOrganizationID returns the requested value for *OrganizationEmailDomain in the common_service_models backend flow.
func (d *OrganizationEmailDomain) GetOrganizationID() OrganizationIdType {
	return d.OrganizationID
}

// GetDomain returns the requested value for *OrganizationEmailDomain in the common_service_models backend flow.
func (d *OrganizationEmailDomain) GetDomain() string {
	return d.Domain
}

// GetCreatedByUserID returns the requested value for *OrganizationEmailDomain in the common_service_models backend flow.
func (d *OrganizationEmailDomain) GetCreatedByUserID() *UserIdType {
	return d.CreatedByUserID
}

// GetCreatedAt returns the requested value for *OrganizationEmailDomain in the common_service_models backend flow.
func (d *OrganizationEmailDomain) GetCreatedAt() time.Time {
	return d.CreatedAt
}

// GetUpdatedAt returns the requested value for *OrganizationEmailDomain in the common_service_models backend flow.
func (d *OrganizationEmailDomain) GetUpdatedAt() time.Time {
	return d.UpdatedAt
}

// OrganizationLike defines the minimum set of methods required for organization objects.
// This interface allows for different organization implementations while maintaining
// a consistent API. The generic type system uses this interface to work with
// various organization types across different layers.
type OrganizationLike interface {
	// GetID returns the organization's unique identifier.
	GetID() OrganizationIdType

	// GetParentOrganizationID returns the optional parent organization ID.
	GetParentOrganizationID() *OrganizationIdType

	// GetLineage returns the ordered list of ancestor org IDs (root -> parent).
	GetLineage() []OrganizationIdType

	// GetName returns the organization's human-readable name.
	GetName() string

	// GetSlug returns the organization's URL-friendly identifier.
	GetSlug() string

	// GetDescription returns the organization's description.
	GetDescription() string

	// GetAvatarUrl returns the object path to the organization's avatar.
	GetAvatarUrl() string

	// GetOrganizationType returns the organization's type/tier.
	GetOrganizationType() proto_models.OrganizationType

	// GetPersonalOwnerUserID returns the authoritative personal-workspace owner.
	// A nil value identifies a shared organization.
	GetPersonalOwnerUserID() *UserIdType

	// GetCreatedAt returns when the organization was created.
	GetCreatedAt() time.Time

	// GetUpdatedAt returns when the organization was last updated.
	GetUpdatedAt() time.Time
}

// UserOrganizationLike defines the minimum set of methods required for user-organization objects.
// This interface allows for different implementations while maintaining a consistent API
// for accessing user membership and role information.
type UserOrganizationLike interface {
	// GetOrganizationID returns the organization's unique identifier.
	GetOrganizationID() OrganizationIdType

	// GetUserID returns the user's unique identifier.
	GetUserID() UserIdType

	// GetExplicitRole returns the user's direct membership role.
	GetExplicitRole() proto_models.OrganizationRole

	// GetEffectiveRole returns the user's role after inheritance resolution.
	GetEffectiveRole() proto_models.OrganizationRole

	// GetIsVirtual returns whether the membership is virtual (inherited).
	GetIsVirtual() bool

	// GetInheritedFromOrganizationID returns the ancestor org that granted the effective role.
	GetInheritedFromOrganizationID() *OrganizationIdType

	// GetEffectiveRoleDepth returns the absolute depth of the org that supplied the effective role.
	GetEffectiveRoleDepth() *uint32

	// GetCreatedAt returns when the user joined the organization.
	GetCreatedAt() time.Time

	// GetUpdatedAt returns when the user's role was last modified.
	GetUpdatedAt() time.Time
}

// Compile-time checks to ensure our models implement their respective interfaces
var _ OrganizationLike = &Organization{}
var _ UserOrganizationLike = &UserOrganization{}

// IsPersonalOrganization reports whether authoritative personal ownership is present.
// Organization type and slug are deliberately ignored.
func IsPersonalOrganization(organization OrganizationLike) bool {
	return organization != nil && organization.GetPersonalOwnerUserID() != nil
}

// IsPersonalWorkspaceRoot reports whether an organization is both personally owned and a root.
func IsPersonalWorkspaceRoot(organization OrganizationLike) bool {
	return IsPersonalOrganization(organization) && organization.GetParentOrganizationID() == nil
}

// IsVirtualMembership evaluates whether the requested condition is true in the common_service_models backend flow.
//
// Parameters:
//   - explicitRole: Input value of type proto_models.OrganizationRole consumed by IsVirtualMembership.
//   - effectiveRole: Input value of type proto_models.OrganizationRole consumed by IsVirtualMembership.
//   - inheritedFromOrganizationID: Identifier passed to IsVirtualMembership to locate the target record.
func IsVirtualMembership(
	explicitRole proto_models.OrganizationRole,
	effectiveRole proto_models.OrganizationRole,
	inheritedFromOrganizationID *OrganizationIdType,
) bool {
	return effectiveRole != explicitRole || inheritedFromOrganizationID != nil
}

// Organization interface implementation

// GetID returns the requested value for *Organization in the common_service_models backend flow.
func (o *Organization) GetID() OrganizationIdType {
	return o.ID
}

// GetParentOrganizationID returns the requested value for *Organization in the common_service_models backend flow.
func (o *Organization) GetParentOrganizationID() *OrganizationIdType {
	return o.ParentOrganizationID
}

// GetLineage returns the requested value for *Organization in the common_service_models backend flow.
func (o *Organization) GetLineage() []OrganizationIdType {
	return o.Lineage
}

// GetName returns the requested value for *Organization in the common_service_models backend flow.
func (o *Organization) GetName() string {
	return o.Name
}

// GetSlug returns the requested value for *Organization in the common_service_models backend flow.
func (o *Organization) GetSlug() string {
	return o.Slug
}

// GetDescription returns the requested value for *Organization in the common_service_models backend flow.
func (o *Organization) GetDescription() string {
	return o.Description
}

// GetAvatarUrl returns the requested value for *Organization in the common_service_models backend flow.
func (o *Organization) GetAvatarUrl() string {
	return o.AvatarUrl
}

// GetOrganizationType returns the requested value for *Organization in the common_service_models backend flow.
func (o *Organization) GetOrganizationType() proto_models.OrganizationType {
	return o.OrganizationType
}

// GetPersonalOwnerUserID returns the authoritative personal-workspace owner, if any.
func (o *Organization) GetPersonalOwnerUserID() *UserIdType {
	if o == nil {
		return nil
	}
	return o.PersonalOwnerUserID
}

// GetCreatedAt returns the requested value for *Organization in the common_service_models backend flow.
func (o *Organization) GetCreatedAt() time.Time {
	return o.CreatedAt
}

// GetUpdatedAt returns the requested value for *Organization in the common_service_models backend flow.
func (o *Organization) GetUpdatedAt() time.Time {
	return o.UpdatedAt
}

// UserOrganization interface implementation

// GetOrganizationID returns the requested value for *UserOrganization in the common_service_models backend flow.
func (uo *UserOrganization) GetOrganizationID() OrganizationIdType {
	return uo.OrganizationID
}

// GetUserID returns the requested value for *UserOrganization in the common_service_models backend flow.
func (uo *UserOrganization) GetUserID() UserIdType {
	return uo.UserID
}

// GetExplicitRole returns the requested value for *UserOrganization in the common_service_models backend flow.
func (uo *UserOrganization) GetExplicitRole() proto_models.OrganizationRole {
	return uo.ExplicitRole
}

// GetEffectiveRole returns the requested value for *UserOrganization in the common_service_models backend flow.
func (uo *UserOrganization) GetEffectiveRole() proto_models.OrganizationRole {
	return uo.EffectiveRole
}

// GetIsVirtual returns the requested value for *UserOrganization in the common_service_models backend flow.
func (uo *UserOrganization) GetIsVirtual() bool {
	return uo.IsVirtual()
}

// IsVirtual evaluates whether the requested condition is true for *UserOrganization in the common_service_models backend flow.
func (uo *UserOrganization) IsVirtual() bool {
	if uo == nil {
		return false
	}
	return IsVirtualMembership(uo.ExplicitRole, uo.EffectiveRole, uo.InheritedFromOrganizationID)
}

// GetInheritedFromOrganizationID returns the requested value for *UserOrganization in the common_service_models backend flow.
func (uo *UserOrganization) GetInheritedFromOrganizationID() *OrganizationIdType {
	return uo.InheritedFromOrganizationID
}

// GetEffectiveRoleDepth returns the requested value for *UserOrganization in the common_service_models backend flow.
func (uo *UserOrganization) GetEffectiveRoleDepth() *uint32 {
	return uo.EffectiveRoleDepth
}

// GetCreatedAt returns the requested value for *UserOrganization in the common_service_models backend flow.
func (uo *UserOrganization) GetCreatedAt() time.Time {
	return uo.CreatedAt
}

// GetUpdatedAt returns the requested value for *UserOrganization in the common_service_models backend flow.
func (uo *UserOrganization) GetUpdatedAt() time.Time {
	return uo.UpdatedAt
}

// CanPromoteToRole evaluates whether the requested operation is allowed in the common_service_models backend flow.
func CanPromoteToRole(managerRole, targetRole proto_models.OrganizationRole) bool {
	if managerRole == proto_models.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10 {
		return true
	}

	// Manager must have strictly higher privilege level to promote to that role
	return managerRole > targetRole
}

// CanManageRole evaluates whether the requested operation is allowed in the common_service_models backend flow.
func CanManageRole(managerRole, targetRole proto_models.OrganizationRole) bool {
	if managerRole == proto_models.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10 {
		return true
	}

	// Manager must have strictly higher privilege level to manage that role
	return managerRole > targetRole
}

// UserOrganizationResponse defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type UserOrganizationResponse struct {
	*UserOrganization
	// User contains the user information if requested.
	User *User `json:"user,omitempty"`
	// Organization contains the organization information if requested.
	Organization *Organization `json:"organization,omitempty"`
}

// OrganizationInvitation defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type OrganizationInvitation struct {
	// ID is the unique identifier for the invitation.
	ID uint64 `json:"id"`

	// OrganizationID is the ID of the organization the user is invited to.
	OrganizationID OrganizationIdType `json:"organization_id"`

	// Email is the email address of the invited user.
	Email string `json:"email"`

	// Role defines the role the user will have upon accepting the invitation.
	Role proto_models.OrganizationRole `json:"role"`

	// InvitedByUserID is the ID of the user who created this invitation.
	InvitedByUserID UserIdType `json:"invited_by_user_id"`

	// CreatedAt indicates when the invitation was created.
	CreatedAt time.Time `json:"created_at"`

	// ExpiresAt indicates when the invitation expires.
	ExpiresAt time.Time `json:"expires_at"`

	// AcceptedAt indicates when the invitation was accepted (nil if pending).
	AcceptedAt *time.Time `json:"accepted_at,omitempty"`

	// DeclinedAt indicates when the invitation was declined (nil if pending or accepted).
	DeclinedAt *time.Time `json:"declined_at,omitempty"`

	// RequiresAcceptance distinguishes invite vs auto-add pending entries.
	RequiresAcceptance bool `json:"requires_acceptance"`

	// Organization contains the organization information if requested.
	Organization *Organization `json:"organization,omitempty"`
}

// GetID returns the requested value for *OrganizationInvitation in the common_service_models backend flow.
func (oi *OrganizationInvitation) GetID() uint64 {
	return oi.ID
}

// GetOrganizationID returns the requested value for *OrganizationInvitation in the common_service_models backend flow.
func (oi *OrganizationInvitation) GetOrganizationID() OrganizationIdType {
	return oi.OrganizationID
}

// GetEmail returns the requested value for *OrganizationInvitation in the common_service_models backend flow.
func (oi *OrganizationInvitation) GetEmail() string {
	return oi.Email
}

// GetRole returns the requested value for *OrganizationInvitation in the common_service_models backend flow.
func (oi *OrganizationInvitation) GetRole() proto_models.OrganizationRole {
	return oi.Role
}

// GetInvitedByUserID returns the requested value for *OrganizationInvitation in the common_service_models backend flow.
func (oi *OrganizationInvitation) GetInvitedByUserID() UserIdType {
	return oi.InvitedByUserID
}

// GetCreatedAt returns the requested value for *OrganizationInvitation in the common_service_models backend flow.
func (oi *OrganizationInvitation) GetCreatedAt() time.Time {
	return oi.CreatedAt
}

// GetExpiresAt returns the requested value for *OrganizationInvitation in the common_service_models backend flow.
func (oi *OrganizationInvitation) GetExpiresAt() time.Time {
	return oi.ExpiresAt
}

// GetAcceptedAt returns the requested value for *OrganizationInvitation in the common_service_models backend flow.
func (oi *OrganizationInvitation) GetAcceptedAt() *time.Time {
	return oi.AcceptedAt
}

// GetDeclinedAt returns the requested value for *OrganizationInvitation in the common_service_models backend flow.
func (oi *OrganizationInvitation) GetDeclinedAt() *time.Time {
	return oi.DeclinedAt
}

// RequiresAcceptanceFlag executes its part of the service logic for *OrganizationInvitation in the common_service_models backend flow.
func (oi *OrganizationInvitation) RequiresAcceptanceFlag() bool {
	return oi.RequiresAcceptance
}

// IsExpired evaluates whether the requested condition is true for *OrganizationInvitation in the common_service_models backend flow.
func (oi *OrganizationInvitation) IsExpired() bool {
	return time.Now().After(oi.ExpiresAt)
}

// OrganizationMemberDetails defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type OrganizationMemberDetails struct {
	// OrganizationID is the ID of the organization.
	OrganizationID OrganizationIdType `json:"organization_id"`

	// UserID is the ID of the user.
	UserID UserIdType `json:"user_id"`

	// ExplicitRole is the user's direct membership role in this organization.
	ExplicitRole proto_models.OrganizationRole `json:"explicit_role"`

	// EffectiveRole is the role after applying lineage inheritance rules.
	EffectiveRole proto_models.OrganizationRole `json:"effective_role"`

	// InheritedFromOrganizationID references the ancestor organization that granted the effective role.
	// Nil when the role is explicit or no inheritance applies.
	InheritedFromOrganizationID *OrganizationIdType `json:"inherited_from_organization_id,omitempty"`

	// EffectiveRoleDepth is the absolute depth of the organization that supplied the effective role (0=root).
	// Nil when EffectiveRole is unspecified.
	EffectiveRoleDepth *uint32 `json:"effective_role_depth,omitempty"`

	// CreatedAt indicates when the user joined the organization.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt indicates when the membership was last modified.
	UpdatedAt time.Time `json:"updated_at"`

	// User profile information
	Username  string                `json:"username"`
	Email     string                `json:"email"`
	FirstName string                `json:"first_name"`
	LastName  string                `json:"last_name"`
	AvatarUrl string                `json:"avatar_url,omitempty"`
	UserType  proto_models.UserType `json:"user_type"`
	Banned    bool                  `json:"banned"`
	Bot       bool                  `json:"is_bot"`

	// Activity and status information
	IsActive     bool       `json:"is_active"`
	LastActivity *time.Time `json:"last_activity,omitempty"`
}

// OrganizationMemberDetailsLike defines the interface for organization member details objects.
type OrganizationMemberDetailsLike interface {
	GetOrganizationID() OrganizationIdType
	GetUserID() UserIdType
	GetExplicitRole() proto_models.OrganizationRole
	GetEffectiveRole() proto_models.OrganizationRole
	GetIsVirtual() bool
	GetInheritedFromOrganizationID() *OrganizationIdType
	GetEffectiveRoleDepth() *uint32
	GetCreatedAt() time.Time
	GetUpdatedAt() time.Time
	GetUsername() string
	GetEmail() string
	GetFirstName() string
	GetLastName() string
	GetAvatarUrl() string
	GetIsActive() bool
	GetLastActivity() *time.Time
}

// Compile-time check to ensure OrganizationMemberDetails implements OrganizationMemberDetailsLike
var _ OrganizationMemberDetailsLike = &OrganizationMemberDetails{}

// GetOrganizationID returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetOrganizationID() OrganizationIdType {
	return m.OrganizationID
}

// GetUserID returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetUserID() UserIdType {
	return m.UserID
}

// GetExplicitRole returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetExplicitRole() proto_models.OrganizationRole {
	return m.ExplicitRole
}

// GetEffectiveRole returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetEffectiveRole() proto_models.OrganizationRole {
	return m.EffectiveRole
}

// GetIsVirtual returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetIsVirtual() bool {
	return m.IsVirtual()
}

// IsVirtual evaluates whether the requested condition is true for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) IsVirtual() bool {
	if m == nil {
		return false
	}
	return IsVirtualMembership(m.ExplicitRole, m.EffectiveRole, m.InheritedFromOrganizationID)
}

// GetInheritedFromOrganizationID returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetInheritedFromOrganizationID() *OrganizationIdType {
	return m.InheritedFromOrganizationID
}

// GetEffectiveRoleDepth returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetEffectiveRoleDepth() *uint32 {
	return m.EffectiveRoleDepth
}

// GetCreatedAt returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetCreatedAt() time.Time {
	return m.CreatedAt
}

// GetUpdatedAt returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetUpdatedAt() time.Time {
	return m.UpdatedAt
}

// GetUsername returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetUsername() string {
	return m.Username
}

// GetEmail returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetEmail() string {
	return m.Email
}

// GetFirstName returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetFirstName() string {
	return m.FirstName
}

// GetLastName returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetLastName() string {
	return m.LastName
}

// GetAvatarUrl returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetAvatarUrl() string {
	return m.AvatarUrl
}

// GetUserType returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetUserType() proto_models.UserType {
	return m.UserType
}

// IsBanned evaluates whether the requested condition is true for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) IsBanned() bool {
	return m.Banned
}

// IsBot evaluates whether the requested condition is true for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) IsBot() bool {
	return m.Bot
}

// GetIsActive returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetIsActive() bool {
	return m.IsActive
}

// GetLastActivity returns the requested value for *OrganizationMemberDetails in the common_service_models backend flow.
func (m *OrganizationMemberDetails) GetLastActivity() *time.Time {
	return m.LastActivity
}
