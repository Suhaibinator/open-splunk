package config

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// UserAttributePermissions is the packed runtime form of one tier's attribute
// visibility: one bit per user attribute a viewer may see. It is what the
// organization and converter layers consult per request; the managed
// configuration spells the same rights out as AttributeVisibility so that
// release documents stay readable.
type UserAttributePermissions uint8

// Permission bits, one per viewable user attribute.
const (
	PermViewEmail UserAttributePermissions = 1 << iota
	PermViewPhoneNumber
	PermViewFirstName
	PermViewLastName
	PermViewUsername
	PermViewAvatarUrl
	PermViewLastActivity
)

// Has reports whether every bit in mask is set.
func (p UserAttributePermissions) Has(mask UserAttributePermissions) bool { return p&mask == mask }

// CanViewEmail reports whether the email attribute is visible.
func (p UserAttributePermissions) CanViewEmail() bool { return p.Has(PermViewEmail) }

// CanViewPhoneNumber reports whether the phone number attribute is visible.
func (p UserAttributePermissions) CanViewPhoneNumber() bool { return p.Has(PermViewPhoneNumber) }

// CanViewFirstName reports whether the first name attribute is visible.
func (p UserAttributePermissions) CanViewFirstName() bool { return p.Has(PermViewFirstName) }

// CanViewLastName reports whether the last name attribute is visible.
func (p UserAttributePermissions) CanViewLastName() bool { return p.Has(PermViewLastName) }

// CanViewUsername reports whether the username attribute is visible.
func (p UserAttributePermissions) CanViewUsername() bool { return p.Has(PermViewUsername) }

// CanViewAvatarUrl reports whether the avatar URL attribute is visible.
func (p UserAttributePermissions) CanViewAvatarUrl() bool { return p.Has(PermViewAvatarUrl) }

// CanViewLastActivity reports whether the last activity attribute is visible.
func (p UserAttributePermissions) CanViewLastActivity() bool { return p.Has(PermViewLastActivity) }

// Union returns the most permissive combination of p and others.
func (p UserAttributePermissions) Union(others ...UserAttributePermissions) UserAttributePermissions {
	for _, other := range others {
		p |= other
	}
	return p
}

// Visibility unpacks the bitfield into its configuration form.
func (p UserAttributePermissions) Visibility() AttributeVisibility {
	return AttributeVisibility{
		Email:        p.CanViewEmail(),
		PhoneNumber:  p.CanViewPhoneNumber(),
		FirstName:    p.CanViewFirstName(),
		LastName:     p.CanViewLastName(),
		Username:     p.CanViewUsername(),
		AvatarUrl:    p.CanViewAvatarUrl(),
		LastActivity: p.CanViewLastActivity(),
	}
}

// AttributeVisibility is the managed-configuration form of one tier's
// attribute view rights. Every field is required in a release document so a
// tier's policy is always spelled out in full.
type AttributeVisibility struct {
	Email        bool `json:"email" yaml:"email" toml:"email"`
	PhoneNumber  bool `json:"phone_number" yaml:"phone_number" toml:"phone_number"`
	FirstName    bool `json:"first_name" yaml:"first_name" toml:"first_name"`
	LastName     bool `json:"last_name" yaml:"last_name" toml:"last_name"`
	Username     bool `json:"username" yaml:"username" toml:"username"`
	AvatarUrl    bool `json:"avatar_url" yaml:"avatar_url" toml:"avatar_url"`
	LastActivity bool `json:"last_activity" yaml:"last_activity" toml:"last_activity"`
}

// Permissions packs the visibility flags into their runtime bitfield.
func (v AttributeVisibility) Permissions() UserAttributePermissions {
	var p UserAttributePermissions
	if v.Email {
		p |= PermViewEmail
	}
	if v.PhoneNumber {
		p |= PermViewPhoneNumber
	}
	if v.FirstName {
		p |= PermViewFirstName
	}
	if v.LastName {
		p |= PermViewLastName
	}
	if v.Username {
		p |= PermViewUsername
	}
	if v.AvatarUrl {
		p |= PermViewAvatarUrl
	}
	if v.LastActivity {
		p |= PermViewLastActivity
	}
	return p
}

// PublicAttributeKey is the GlobalUserAttributePermissions key that governs
// unauthenticated viewers (UserType UNSPECIFIED).
const PublicAttributeKey = "public"

const (
	organizationRoleKeyPrefix = "ORGANIZATION_ROLE_"
	userTypeKeyPrefix         = "USER_TYPE_"
)

// OrganizationRoleAttributeKey returns the OrgUserAttributePermissions key for
// role: its proto enum name without the ORGANIZATION_ROLE_ prefix, lowercased
// (for example "member_tier_1"). UNSPECIFIED has no key and yields "".
func OrganizationRoleAttributeKey(role proto_models.OrganizationRole) string {
	if role == proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		return ""
	}
	return enumAttributeKey(proto_models.OrganizationRole_name, int32(role), organizationRoleKeyPrefix)
}

// ParseOrganizationRoleAttributeKey resolves a canonical OrgUserAttributePermissions
// key back to its role. It reports false for unknown keys, non-canonical
// spellings, and the UNSPECIFIED role.
func ParseOrganizationRoleAttributeKey(key string) (proto_models.OrganizationRole, bool) {
	value, ok := parseEnumAttributeKey(proto_models.OrganizationRole_value, key, organizationRoleKeyPrefix)
	if !ok || value == int32(proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED) {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, false
	}
	return proto_models.OrganizationRole(value), true
}

// UserTypeAttributeKey returns the GlobalUserAttributePermissions key for
// userType: PublicAttributeKey for UNSPECIFIED, otherwise the proto enum name
// without the USER_TYPE_ prefix, lowercased (for example "admin_tier_1").
func UserTypeAttributeKey(userType proto_models.UserType) string {
	if userType == proto_models.UserType_USER_TYPE_UNSPECIFIED {
		return PublicAttributeKey
	}
	return enumAttributeKey(proto_models.UserType_name, int32(userType), userTypeKeyPrefix)
}

// ParseUserTypeAttributeKey resolves a canonical GlobalUserAttributePermissions
// key back to its user type. PublicAttributeKey maps to UNSPECIFIED; the
// literal "unspecified" is rejected so the public policy has exactly one
// spelling.
func ParseUserTypeAttributeKey(key string) (proto_models.UserType, bool) {
	if key == PublicAttributeKey {
		return proto_models.UserType_USER_TYPE_UNSPECIFIED, true
	}
	value, ok := parseEnumAttributeKey(proto_models.UserType_value, key, userTypeKeyPrefix)
	if !ok || value == int32(proto_models.UserType_USER_TYPE_UNSPECIFIED) {
		return proto_models.UserType_USER_TYPE_UNSPECIFIED, false
	}
	return proto_models.UserType(value), true
}

func enumAttributeKey(names map[int32]string, value int32, prefix string) string {
	name, ok := names[value]
	if !ok {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(name, prefix))
}

func parseEnumAttributeKey(values map[string]int32, key, prefix string) (int32, bool) {
	if key == "" || key != strings.ToLower(key) {
		return 0, false
	}
	value, ok := values[prefix+strings.ToUpper(key)]
	return value, ok
}

// validateAttributeVisibilityKeys rejects keys that do not name a tier. It is
// the release-time guard: a misspelled key would otherwise silently grant
// nothing to the tier the operator meant to configure.
func validateAttributeVisibilityKeys(field string, byKey map[string]AttributeVisibility, parse func(string) bool) error {
	invalid := make([]string, 0)
	for key := range byKey {
		if !parse(key) {
			invalid = append(invalid, key)
		}
	}
	if len(invalid) == 0 {
		return nil
	}
	sort.Strings(invalid)
	return fmt.Errorf("%s has unknown tier keys %q", field, invalid)
}

func validateOrganizationRoleAttributeKeys(field string, byKey map[string]AttributeVisibility) error {
	return validateAttributeVisibilityKeys(field, byKey, func(key string) bool {
		_, ok := ParseOrganizationRoleAttributeKey(key)
		return ok
	})
}

func validateUserTypeAttributeKeys(field string, byKey map[string]AttributeVisibility) error {
	return validateAttributeVisibilityKeys(field, byKey, func(key string) bool {
		_, ok := ParseUserTypeAttributeKey(key)
		return ok
	})
}

type attributeEntry struct {
	permissions UserAttributePermissions
	configured  bool
}

// AttributePermissionTable is the compiled runtime lookup for one
// AttributeVisibility map: a dense table indexed directly by the proto enum
// value, so a lookup is one bounds check and one load. Tables are immutable
// after compilation and safe to share across goroutines.
type AttributePermissionTable[K ~int32] struct {
	entries []attributeEntry
}

// GetPermissions returns the permissions configured for key, or nil when the
// key has no entry. The pointer aliases the table and must not be written to.
func (t AttributePermissionTable[K]) GetPermissions(key K) *UserAttributePermissions {
	index := int32(key)
	if index < 0 || int(index) >= len(t.entries) {
		return nil
	}
	entry := &t.entries[index]
	if !entry.configured {
		return nil
	}
	return &entry.permissions
}

// OrgRoleAttributePermissions is the compiled lookup for one depth of
// OrgUserAttributePermissions.
type OrgRoleAttributePermissions = AttributePermissionTable[proto_models.OrganizationRole]

// UserTypeAttributePermissions is the compiled lookup for
// GlobalUserAttributePermissions.
type UserTypeAttributePermissions = AttributePermissionTable[proto_models.UserType]

var (
	organizationRoleTableSize = enumTableSize(proto_models.OrganizationRole_name)
	userTypeTableSize         = enumTableSize(proto_models.UserType_name)
)

func enumTableSize(names map[int32]string) int {
	size := 0
	for value := range names {
		if int(value)+1 > size {
			size = int(value) + 1
		}
	}
	return size
}

func compileAttributePermissions[K ~int32](byKey map[string]AttributeVisibility, size int, parse func(string) (K, bool)) AttributePermissionTable[K] {
	if len(byKey) == 0 {
		return AttributePermissionTable[K]{}
	}
	entries := make([]attributeEntry, size)
	for key, visibility := range byKey {
		value, ok := parse(key)
		if !ok {
			// Validate rejects unknown keys before a generation is published;
			// an unvalidated static config simply skips them.
			continue
		}
		entries[int32(value)] = attributeEntry{permissions: visibility.Permissions(), configured: true}
	}
	return AttributePermissionTable[K]{entries: entries}
}

// CompileOrgRoleAttributePermissions builds the runtime lookup for one depth
// of OrgUserAttributePermissions.
func CompileOrgRoleAttributePermissions(byKey map[string]AttributeVisibility) OrgRoleAttributePermissions {
	return compileAttributePermissions(byKey, organizationRoleTableSize, ParseOrganizationRoleAttributeKey)
}

// CompileOrgRoleAttributePermissionsByDepth builds the runtime lookups for
// every depth of OrgUserAttributePermissions; index i is depth i.
func CompileOrgRoleAttributePermissionsByDepth(byDepth []map[string]AttributeVisibility) []OrgRoleAttributePermissions {
	if byDepth == nil {
		return nil
	}
	tables := make([]OrgRoleAttributePermissions, len(byDepth))
	for depth, byKey := range byDepth {
		tables[depth] = CompileOrgRoleAttributePermissions(byKey)
	}
	return tables
}

// CompileUserTypeAttributePermissions builds the runtime lookup for
// GlobalUserAttributePermissions.
func CompileUserTypeAttributePermissions(byKey map[string]AttributeVisibility) UserTypeAttributePermissions {
	return compileAttributePermissions(byKey, userTypeTableSize, ParseUserTypeAttributeKey)
}
